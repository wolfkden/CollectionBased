require 'yaml'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
require 'stringio'
require './job_spec.rb'
require './machine.rb'

class CollectionDispatcher
  attr_reader :bucket, :bucket_name, :queues, :dispatched,
    :s3_file_array, :task_yaml_base

  def initialize(jobSpec=nil)
    super()

    @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
    if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec) then return end

#    machines.each { |m| puts "Machine_id: #{m.machine_id}" }
#    machines.first.sessions.each { |s| puts "Session id: #{s.session_id}" }
#    machines.first.sessions.first.collections.each { |c| puts "Collection id: #{c.collection_id}" }
#    machines.first.sessions.first.collections.first.tasks.each { |t| puts "Task id: #{t.task_id}" }
#    task = machines.first.sessions.first.collections.first.tasks.first
#    puts "task base directory: #{task.base_directory}"
#    puts "Work unit: #{work_unit(task)}"
#    puts "Task Sequence: #{machines.first.sessions.first.collections.first.task_sequence}"
#    upload_file(task)
#    collection = machines.first.sessions.first.collections.first

    machines.each { |m| queue_worker(m)  }
#    queue_worker(collection)
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

  def queues
    @queues = @queues ? @queues : @jobSpec.queues
  end

  def bucket
    @bucket = @bucket ? @bucket : @jobSpec.bucket
  end

  def bucket_name
    @bucket_name = @bucket_name ? @bucket_name : @jobSpec.bucket_name
  end

  def work_unit(task)
    return {} if !task.instance_of? Task || !@jobSpec

		{
      :created_at => Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
      :s3_download => task.task_files.map { |f| File.join(bucket_name, f.gsub(/^\.\//, '')) },
      :serial => task.task_id,
      :r_collection_id => task.collection.collection_id.to_s,
      :r_serial_id => task.collection.session.session_id.to_s,
      :r_machine_id => task.collection.session.machine.machine_id.to_s,
		}

  end

  # Puts a message in the SQS queue
  def enqueue(queueKey, work_unit)
    queues[queueKey].send_message(work_unit)
  end

  # This stores data in the bucket and key(path)
  def upload_file(task)
    return if bucket.nil? || !task.instance_of?(Task)
    task.upload_files.each { |f|
      begin
        file_ptr = File.exist?(f) ? File.new(f) : ""
        bucket.put(f.gsub(/^\.\//, ''), file_ptr)
      rescue Exception => e
        puts "Cannot open file named: #{f} Message: #{e}"
      end

    }
  end

  def queue_worker(task=nil)

    return machines.each { |m| queue_worker(m) } if task.nil?
    return task.sessions.each { |s| queue_worker(s) } if task.instance_of?(Machine)
    return task.collections.each { |c| queue_worker(c) } if task.instance_of?(Session)
    return task.tasks.each { |t| queue_worker(t) } if task.instance_of?(Collection)

    self.upload_file(task)
#    puts work_unit(task)
    sndmsg = enqueue(QUEUE::INPUT, work_unit(task).to_yaml)

    debug_txt =
      "   #{task.task_id}  ====>: MsgID: #{task.base_directory}"
    @jobSpec.log_message(debug_txt)

  rescue Exception => e
      STDERR.puts e
      @jobSpec.log_message(e)
      raise "Job Unit Deployment Failure"
  end
end

#CollectionDispatcher.new