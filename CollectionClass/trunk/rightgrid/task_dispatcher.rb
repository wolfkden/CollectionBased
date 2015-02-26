require 'yaml'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
#require '/var/spool/ec2/meta-data-cache.rb'
#require '/var/spool/ec2/user-data.rb'
require 'stringio'
require './job_spec.rb'

class TaskDispatcher < StringIO
  attr_accessor :maxLineCount, :baseName, :lc,
    :r_function, :r_preamble, :r_postprocess, :r_array_size
  attr_reader :bucket, :bucketName, :queues, :workUnit, :serialID, 
    :dispatched, :s3_file_array, :task_yaml_base

  def initialize(jobSpec, baseName, maxLineCount = 2**6+2**5) # 2**15)
    super()

    @jobSpec = jobSpec
    @baseName = baseName && baseName.length ?
      baseName : @jobSpec.uploadedFiles.first
    @codeName = @jobSpec.codeFiles.length ? @jobSpec.codeFiles.first : nil
    @maxLineCount = maxLineCount

    @jobSpec.logMessage("SerialID: #{self.serialID}")

    @task_yaml_base = {}

  end

  def s3_file_array
     @s3_file_array = []
    @s3_file_array.push(File.join(@jobSpec.bucketName, self.dispatched.fileName))
    if(@jobSpec.codeFiles.length)
      @s3_file_array.push(File.join(@jobSpec.bucketName, @jobSpec.codeFiles.first))
    end
  end


  def dispatched
    @dispatched = @dispatched ? @dispatched : Dispatched.new(self.baseName)
  end

  def lc
    @lc = @lc ? @lc : 0
  end

  def queues
    @queues = @queues ? @queues : @jobSpec.queues
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
			  :s3_download => self.s3_file_array,
			  :serial => self.serialID,
        :r_function => self.r_function,
        :r_preamble => self.r_preamble,
        :r_postprocess => self.r_postprocess,
		}
  end

  def serialID
    @serialID = "#{@jobSpec.rrpid}_#{self.dispatched.count}"
  end

  def r_preamble
    @r_preamble = @r_preamble ? @r_preamble : @jobSpec.r_preamble
  end

  def r_postprocess
    @r_postprocess = @r_postprocess ? @r_postprocess : @jobSpec.r_postprocess.strip
  end

  def r_array_size
    @r_array_size = @r_array_size ? @r_array_size : @jobSpec.r_array_size.to_i
  end

  def r_function
    @r_function = @r_function ? @r_function : @jobSpec.r_function
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

    if self.string.length < 0 then return end
#    if self.string.length < 1 then return end

    self.uploadFile
    sndmsg = self.enqueue(QUEUE::INPUT, self.workUnit.to_yaml)

    @task_yaml_base["#{self.serialID}"] = self.workUnit.to_yaml

    debug_txt =
      "     ====>: #{self.serialID} MsgID: #{self.dispatched.count}"
    @jobSpec.logMessage(debug_txt)

    self.lc = 0
    self.dispatched.nextCount
#    @s3_file_array = nil
  rescue Exception => e
      STDERR.puts e
      @jobSpec.logMessage(e)
      raise "Job Unit Deployment Failure"
  end

  def clear
    self.truncate(0)
    self.rewind
  end

  def taskCount
    self.dispatched.count
  end

  def line_nil_postprocess
    increment = self.r_array_size.to_i < 1 ? 30 : (self.r_arra
      y_size + 10*(self.r_array_size % 10 == 0 ? 0 : 1))/ 10
    self.r_postprocess = "aa <- c(#{self.taskCount*increment}:#{(self.taskCount+1)*increment-1});"
#    puts "Increment #{increment} r_postprocess #{r_postprocess}"
  end

  def <<(line)
#    line_nil_postprocess if line.nil?
    return self.queueWorker if line.nil?
    super

    self.queueWorker if (line.length == 0 && 0 < self.string.length) ||
        (self.lc = self.lc + 1) == self.maxLineCount
      
  end

  class Dispatched
    attr_reader :baseName, :fileName, :count
    def initialize(baseName = "")
      @count = 0
      @baseName = baseName
    end

    def fileName
      @fileName = "#{self.baseName}_#{self.count}"
    end

    def nextFileName
      @fileName = "#{self.baseName}_#{self.nextCount}"
    end

    def nextCount
      @count = @count + 1
    end

  end
end
