require 'yaml'
require 'pathname'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
require './job_spec.rb'
require './machine.rb'

class CollectionCollector
  attr_reader :bucket, :queues, :sleeptime, :i_queue, :o_queue, :e_queue, :a_queue,
    :dispatched, :task, :collection, :session, :machine, :taskProcessed, :auditProcessed

  def initialize(jobSpec = nil)

    @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
    if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec) then return end

    @sleeptime = 2

#    self.showQueues

#    puts self.bucketSelect("^in\/9160_")

    puts "Jobs Processed: #{taskProcessed.length}"

    self.processResults

#    self.processAudits

    rescue Exception => e
      write_file

        STDERR.puts e
        @jobSpec.log_message(e)
        raise "Task Unit Collection Failure"
  end

  def write_file(file_data=nil, file_name=nil, mode=nil)

    collection.update if file_name.nil? && collection.instance_of?(Collection)
    file_name = @jobSpec.taskProcessedList if file_name.nil?

    file_name = File.join(collection.instance_of?(Collection) ?
        collection.base_directory : ".", file_name.nil? ?
        @jobSpec.taskProcessedList : file_name)

    Dir.mkdir(File.dirname(file_name)) if !File.directory?(File.dirname(file_name))

#    puts "Writing file: #{file_name}"
    mode = "w+" if mode.nil?

    File.open(file_name, mode) { |f| 
      f.write(file_data.nil? ? taskProcessed.to_yaml : file_data) }
  end

  def processResults(collection = nil)
    return machines.each { |m| processResults(m) } if collection.nil?
    return collection.sessions.each { |s| processResults(s) } if collection.instance_of?(Machine)
    return collection.collections.each { |c| processResults(c) } if collection.instance_of?(Session)
    self.collection = collection if collection.instance_of? Collection

    while(0 < collection.tasks.length)
#      showQueues
      while(msg = o_queue.pop)
        processOutputMessage(msg)
      end

      while(msg = e_queue.pop)
        processErrorMessage(msg)
      end
    end

    puts "#{taskProcessed.length} messages processed"
    write_file
  end

  def processAudits

    while( 0 < a_queue.size) # ||
#          auditProcessed.length < taskProcessed.length)
#      print Array.new(88).fill(" ", 0...80).join.concat("\r")
      print "Audits Processed #{auditProcessed.length} queue size: #{a_queue.size}\r"
      while(msg = a_queue.pop)
        processAuditMessage(msg)
      end
    end

  end

  def processAuditMessage(message)

    decodemsg = YAML.load(message.body)

    # Get data to log
    a_serial = ""
    if defined? decodemsg[:audit_info][:serial]
      a_serial = "#{decodemsg[:audit_info][:serial]}"
    end

#      puts "Audit Process #{auditProcessed.length} serial ID: #{a_serial}\r"

   return nil if (auditProcessed a_serial, decodemsg, QUEUE::AUDIT).nil?

#      puts "Audit Processing #{auditProcessed.length} serial ID: #{a_serial}\r"

    a_worker_result = ""
    if defined? decodemsg[:worker_result]
      a_worker_result = "#{decodemsg[:worker_result]}"
    end
    a_result = ""
    if defined? decodemsg[:result]
      a_result = "#{decodemsg[:result]}"
    end
    a_work_id = ""
    if defined? decodemsg[:work_item_id]
      a_work_id = "#{decodemsg[:work_item_id]}"
    end
    a_result_id = ""
    if defined? decodemsg[:result_item_id]
      a_result_id = "#{decodemsg[:result_item_id]}"
    end
    a_worksecs = ""
    if defined? decodemsg[:secs_to_work]
      a_worksecs = "#{decodemsg[:secs_to_work]}"
    end
    a_downloadsecs = ""
    if defined? decodemsg[:secs_to_download]
      a_downloadsecs = "#{decodemsg[:secs_to_download]}"
    end

    a_uploadsecs = ""
    if defined? decodemsg[:secs_to_upload]
      a_uploadsecs = "#{decodemsg[:secs_to_upload]}"
    end

    a_created_at = ""
    if defined? decodemsg[:work_unit_created_at]
      a_created_at = "#{decodemsg[:work_unit_created_at]}"
    end

    #  2. For example, log data to a file or update a central DB with statistics
    a_data = "#{a_serial},#{a_work_id},#{a_result_id},#{a_worker_result},#{a_result},#{a_worksecs},#{a_downloadsecs},#{a_uploadsecs},#{a_created_at}\n"
    write_file(a_data, 'output/audit.csv','a+')

    #  3. Some Debug Output
    @jobSpec.log_message("Audit Processing: SerialID: #{a_serial} Msg ID: #{message.id}")
    "Processed Audit Message: Serial ID =  #{a_serial}"

  end

  def processOutputMessage(message)

    puts "Processed #{taskProcessed.length} Output Results remaining in queue #{collection.tasks.length}"

    decodemsg = YAML.load(message.body)

    a_serial = ""
    if defined? decodemsg[:audit_info][:serial]
      a_serial = "#{decodemsg[:audit_info][:serial]}"
    end

   puts "Processing task serial_id: #{a_serial}"
   collection_size = defined? decodemsg[:collection_size] ? decodemsg[:collection_size] : ""

   return nil if(taskProcessed a_serial, decodemsg, QUEUE::OUTPUT).nil?

    a_created_at = ""
    if defined? decodemsg[:created_at]
      a_created_at = "#{decodemsg[:created_at]}"
    end

    # If there are any output files, download them to the local output directory
    if defined? decodemsg["s3_upload"]
      s3_upload = decodemsg["s3_upload"]
#      d_bucket = self.findBucket(s3_upload[s3_upload.keys.first].gsub(/\(/, '\(').gsub(/\)/, '\)').split('/').first)
      #d_key=s3_upload[s3_upload.keys.first].gsub(/\(/, '\(').gsub(/\)/, '\)').split('/')[1..-1].join('/')
#      d_key=URI.encode(s3_upload[s3_upload.keys.first].gsub(/\(/, '\(').gsub(/\)/, '\)').gsub(s3_upload[s3_upload.keys.first].gsub(/\(/, '\(').gsub(/\)/, '\)').split('/').first,"").gsub('/out','out'))
#
#      write_file(d_bucket.get(d_key), File.join("output", File.basename(d_key)))
#
#d_bucket.move_key(d_key, File.join(collection.base_directory, File.basename(d_key, suffix)))

      # Some Debug Output
      @jobSpec.log_message("Output Processing: serial ID: #{a_serial} Msg ID: #{message.id} created_at: #{a_created_at}")
      msgline = "Processed Output Message - Serial id = #{a_serial}"
    else
      @jobSpec.log_message("No Output Files: serial ID: #{a_serial} Msg ID: #{message.id}")
    end

    self.taskProcessed[a_serial]
  end

  def processErrorMessage(message)

    puts "Processed #{taskProcessed.length} Output Results remaining in queue #{collection.tasks.length}"

    decodemsg = YAML.load(message.body)

    orig_msg=YAML.load(decodemsg["message"])

    e_serial = ""
    if defined? orig_msg[:serial]
      e_serial = "#{orig_msg[:serial]}"
    end

   puts "Processing task serial_id: #{e_serial}"
   return nil if (taskProcessed e_serial, decodemsg, QUEUE::ERROR).nil?

puts "processing Error serial ID #{e_serial}"

    e_created_at = ""
    if defined? orig_msg[:created_at]
      e_created_at = "#{orig_msg[:created_at]}"
    end

    e_conversion_type = ""
    if defined? orig_msg[:conversion_type]
      e_conversion_type = "#{orig_msg[:conversion_type]}"
    end

    e_worker_name = ""
    if defined? orig_msg[:worker_name]
      e_worker_name = "#{orig_msg[:worker_name]}"
    end

    e_s3_download = ""
    if defined? orig_msg[:s3_download]
      e_s3_download = "#{orig_msg[:s3_download]}"
    end

    if (e_conversion_type == "red")
      # We can only correct errors where convesion_type = red

      # Construct a Work_Unit
      # a work_unit can have any number of elements but must have the following 2 elements
      #   created_at
      #   s3_download
      work_unit = {
          :created_at => Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
          :s3_download => e_s3_download,
          :serial => e_serial,
          :conversion_type => "sep"
      }

      # Debug print
      puts work_unit

      #  Add worker_name if it exists
      # The worker_name which corresponds to a ruby class on the worker instance
      if (!e_worker_name.empty? && !e_worker_name.nil?)
          work_unit[:worker_name] = "#{e_worker_name}"
      end

      # Encode a work_unit using YAML and place the resulting message in the :inputqueue
      wu_yaml = work_unit.to_yaml
      sndmsg = enqueue(QUEUE::INPUT, wu_yaml)

      # We have processed 1 error message, which will increase the expected audit and output sizes by 1
      add_to_data(1, "worksize.dat")

      #simple error logic: reset conversion type to sep
      @jobSpec.log_message("ReProcessing serial ID: #{e_serial} Msg ID: #{message.id} - Changing #{e_conversion_type} to sep")

    else
      @jobSpec.log_message("dequeued Error Message serial ID: #{e_serial} Msg ID: #{message.id} - do Nothing")
    end

    # Update the Display
    msgline = "Processed Error Message - Serial Number = #{e_serial} "

    self.taskProcessed[e_serial]
  end

  def machine_keys
    dispatched.keys
  end

  def machines
    dispatched.keys.map { |k| machine(k) }
  end

  def machine(machine_id)
    Machine.new(machine_list(machine_id), @jobSpec, self)
  end

  def machine_list(machine_id)
    { machine_id => dispatched[machine_id] }
  end

  def session_keys(machine_id)
    dispatched[machine_id].keys
  end

  def dispatched
    @dispatched = @dispatched ? @dispatched : YAML::load_file(@jobSpec.taskDispatchList)
    @dispatched = @dispatched.instance_of?(Hash) ? @dispatched : {}
  end

  def session=(session)
    @session=session
  end

  def session
    @session
  end

  def collection=(collection)
    @collection=collection
  end

  def collection
    @collection
  end

  # Puts a message in the SQS queue
  def enqueue(queueKey, workUnit)
    self.queues[queueKey].send_message(workUnit)
  end

  def dequeue(queue)
    self.queues[queue].pop
  end

  def auditProcessed(key=nil, message=nil, type=nil)
    @auditProcessed = @auditProcessed.instance_of?(Hash) ?  @auditProcessed : {}
    return @auditProcessed if key.nil?
    return @auditProcessed[key] if message.nil? || type.nil?
    @auditProcessed[key] = { :message => message, :type => type } # if !taskProcessed[key].nil?
  end

  def load_processed_record
    collection.instance_of?(Collection) && collection.tasks.length < 1 &&
    File.exists?(processed_file = File.join(collection.base_directory, @jobSpec.taskProcessedList) ) ?
          YAML::load_file(processed_file) : {}
  end

  def taskProcessed(key=nil, message=nil, type=nil)
    @taskProcessed = @taskProcessed.instance_of?(Hash) ?
      @taskProcessed : load_processed_record
    return @taskProcessed if key.nil?
    return @taskProcessed[key] if message.nil? || type.nil?
    @taskProcessed[key] = { :message => message, :type => type } if collection.delete(key)
  end

  def bucketSelect(regex)

    list = []; self.bucket.keys.each{ |x| x = "#{x}"; list.push x if x =~ Regexp.new(regex) }
    list
  end

  # Write the results page (index.html)
  def showQueues
    print Array.new(88).fill(" ", 0...80).join.concat("\r")
    print "input: #{i_queue.size} output: #{o_queue.size} error: #{e_queue.size} audit: #{a_queue.size}\r"

  end

  def queues
    @queues = @queues ? @queues : @jobSpec.queues
  end

  def i_queue
    @i_queue = @i_queue ? @i_queue : self.queues[QUEUE::INPUT]
  end

  def o_queue
    @o_queue = @o_queue ? @o_queue : self.queues[QUEUE::OUTPUT]
  end

  def e_queue
    @e_queue = @e_queue ? @e_queue : self.queues[QUEUE::ERROR]
  end

  def a_queue
    @a_queue = @a_queue ? @a_queue : self.queues[QUEUE::AUDIT]
  end

  def s3
    @s3 = @s3 ? @s3 : @jobSpec.s3
  end

  def sqs
    @sqs = @sqs ? @sqs : @jobSpec.sqs
  end

  def bucket
    @bucket = @bucket ? @bucket : @jobSpec.bucket
  end

  def findBucket(key = nil)
    self.s3.bucket(key, false) if key
  end

  def bucket_name
    @bucket_name = @bucket_name ? @bucket_name : @jobSpec.bucket_name
  end

end

#CollectionCollector.new