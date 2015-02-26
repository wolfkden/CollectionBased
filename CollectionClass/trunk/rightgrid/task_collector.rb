require 'right_aws'
require 'uri'
require 'net/smtp'
require '/var/spool/ec2/meta-data-cache.rb'
require '/var/spool/ec2/user-data.rb'
require 'stringio'
require 'job_spec'

class TaskCollector
  attr_reader :bucket, :bucketName, :queues, :workUnit, :serialID, :dispatched

  def initialize(jobSpec, baseName)

    @jobSpec = jobSpec ? jobSpec : JobSpec.new

    @baseName = baseName && baseName.length ?
      baseName : @jobSpec.uploadedFiles.first
    @jobSpec.logMessage("SerialID: #{self.serialID}")
    
  end

  # this retrieves and deletes the next queued message
  def dequeue_entry(queueKey)
     self.queues[queueKey].pop
  end

  # Puts work in the queue
  def enqueue_work_unit(queueKey, workUnit)
    self.queues[queueKey].send_message(workUnit)
  end

  # Pops existing messages off the queue to clear the queue
  def clear_queue(queueKey, qname)
      puts ("Clearing Queue #{qname}")
      while ( (msg = dequeue_entry(self.queues[queueKey])) != nil )
        puts ("Popping message #{msg.id}")
      end
  end

  def queues
    @queuses = @queues ? @queues : @jobSpec.queues
  end

  def bucket
    @bucket = @bucket ? @bucket : @jobSpec.bucket
  end

  def bucketName
    @bucketName = @bucketName ? @bucketName : @jobSpec.bucketName
  end

  def workUnit
    if !@jobSpec || !self.serialID then return end

		@workUnit = {
			  :created_at => Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
			  :s3_download => [File.join(@jobSpec.bucketName, self.dispatched.fileName)],
			  :serial => self.serialID,
		}

  end

  def serialID
    @serialID = "#{@jobSpec.rrpid}_#{self.dispatched.count}"
  end

  # Keep track of various work
  def add_to_data(audit_add, afilename)
    if File::exists?(afilename)
      datafile = File.new(afilename, File::RDONLY|File::CREAT)
    else
      datfile = File.new(afilename, "w")
      datfile.puts("0")
      datfile.close
      datafile = File.new(afilename, File::RDONLY|File::CREAT)
    end
    anumtxt = datafile.gets.chomp
    datafile.close
    anum = anumtxt.to_i
    anum = anum + audit_add.to_i
    if (audit_add.to_i > 0)
      puts "     + #{audit_add} = #{anum} #{afilename} "
    end
    anums = anum.to_s
    datafile = File.new(afilename, "w")
    datafile.puts(anums)
    datafile.close
    return anum
  end

  # Puts a message in the SQS queue
  def enqueue(queueKey, workUnit)
    self.queues[queueKey].send_message(workUnit)
  end

  # This stores data in the bucket and key(path)
  def uploadFile(key = "")
    key = 0 < key.length ? key : self.dispatched.fileName
    self.bucket.put(key, self.string)
    self.clear
  end

  def queueWorker

    if self.string.length < 1 then return end

    self.uploadFile
    sndmsg = self.enqueue(QUEUE::INPUT, self.workUnit.to_yaml)

    debug_txt =
      "     ====>: #{self.serialID} MsgID: #{self.dispatched.count}"
    @jobSpec.logMessage(debug_txt)

    self.dispatched.nextCount
  rescue Exception => e
      STDERR.puts e
      @jobSpec.logMessage(e)
      raise "Job Unit Deployment Failure"
  end

end
