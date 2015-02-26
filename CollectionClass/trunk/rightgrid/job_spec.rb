require 'yaml'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
require 'uuidtools'
#require '/var/spool/ec2/meta-data-cache.rb'
#require '/var/spool/ec2/user-data.rb'
require './enumerable.rb'

class QUEUE < ENUM
  QUEUE.add_item :INPUT, "Input", 1
  QUEUE.add_item :OUTPUT, "Output", 2
  QUEUE.add_item :AUDIT, "Audit",  4
  QUEUE.add_item :ERROR, "Error",  8
end

class JobSpec
  attr_accessor :codeFiles, :uploadedFiles, :jobspecFilename, :segmentCount,
    :r_function, :r_preamble, :r_postprocess, :taskDispatchList
  attr_reader :queues, :rrpid, :bucket, :bucket_name, :bucket_base, :bucketBaseName,
    :access_key_id, :secret_access_key, :jobspec

  def initialize(jobspecFilename = "jobspec.yml")

    @jobspecFilename = File::exist?(jobspecFilename) ? jobspecFilename : "jobspec.yml"
    @jobspec = YAML::load_file(@jobspecFilename)

    log_message("Yaml File List: #{@jobspecFilename}")

    @uploadedFiles = @jobspec[:uploadedFile]

  end

  def rrpid
    @rrpid = @rrpid ? @rrpid : @jobspec[:ruby_pid]
  end

  def set_rrpid
   @jobspec[:ruby_pid] = @rrpid = $$
  end

  def access_key_id
    @access_key_id = @access_key_id ? @access_key_id : @jobspec[:access_key_id]
  end

  def secret_access_key
    @secret_access_key = @secret_access_key ? @secret_access_key : @jobspec[:secret_access_key]
  end

  def taskDispatchList
    @taskDispatchList = @taskDispatchList ? @taskDispatchList :
      (@jobspec[:task_dispatch_list] ? @jobspec[:task_dispatch_list] : "dispatched.yml")
  end

  def taskDispatchList=(value)
    @taskDispatchList = @jobspec[:task_dispatch_list] = value
  end

  def taskProcessedList
    @taskProcessedList = @taskProcessedList ? @taskProcessedList :
        (@jobspec[:task_processed_list] ? @jobspec[:task_processed_list] : "processed.yml")
  end

  def taskProcessedList=(value)
    @taskProcessedList = @jobspec[:task_processed_list] = value
  end

  def codeFiles
    @codeFiles = @codeFiles ? @codeFiles :
      (@jobspec[:codeFile] && @jobspec[:codeFile].length ? @jobspec[:codeFile] : [])
  end

  def r_function=(value)
    @jobspec[:r_function] = @r_function = value
  end

  def r_function
    @r_function = @r_function ? @r_function : 
      (@jobspec[:r_function] && @jobspec[:r_function].length ?
        @jobspec[:r_function] : "function(x,y, fn) fn(x) * y, y=3, fn<-function(z) z + 1")
  end

  def r_array_size=(value)
    @jobspec[:r_array_size] = @r_array_size = value
  end

  def r_array_size
    @r_array_size = @r_array_size ? @r_array_size :
      (@jobspec[:r_array_size] ? @jobspec[:r_array_size] : "").to_i
  end

  def r_preamble=(value)
    @jobspec[:r_preamble] = @r_preamble = value
  end

  def r_preamble
    @r_preamble = @r_preamble ? @r_preamble :
      (@jobspec[:r_preamble] ? @jobspec[:r_preamble] : "")
  end

  def r_postprocess=(value)
    @jobspec[:r_postprocess] = @r_postprocess = value
  end

  def r_postprocess
    @r_postprocess = @r_postprocess ? @r_postprocess :
      (@jobspec[:r_postprocess] ? @jobspec[:r_postprocess] : "")
  end

  def bucket_name
    @bucket_name = @bucket_name ? @bucket_name : @jobspec[:bucket]
  end

  def bucketBaseName
    @bucketBaseName = @bucketBaseName ? @bucketBaseName : @jobspec[:bucket_base]
  end

  def s3
    # Get S3
#    if !@s3 then self.log_message("S3 Bucket: #{@jobspec[:bucket]}") end
    @s3 = @s3 ? @s3 : RightAws::S3.new(self.access_key_id, self.secret_access_key)
  end

  def bucket
    @bucket = @bucket ? @bucket : s3.bucket(self.bucket_name, false)
  end

  def bucket_base
    @bucket_base = @bucket_base ? @bucket_base : s3.bucket(self.bucketBaseName, false)
  end

  def sqs
    @sqs = @sqs ? @sqs : RightAws::SqsGen2.new(self.access_key_id, self.secret_access_key)
  end

  def queues(key = nil)
    @queues = @queues == nil ? {} : @queues
    if key
#      puts "JobSpec Accessign AWS queue #{key}"
      @queues[key] = case key
      when QUEUE::INPUT  then sqs.queue(@jobspec[:inputqueue],  false)
      when QUEUE::OUTPUT then sqs.queue(@jobspec[:outputqueue], false)
      when QUEUE::AUDIT  then sqs.queue(@jobspec[:auditqueue],  false)
      when QUEUE::ERROR  then sqs.queue(@jobspec[:errorqueue],  false)
      else raise "Unknown SQS Queue"
      end
    else
      # Get SQS Handles
      if @queues.length == 0
        self.queues(QUEUE::INPUT)
        self.queues(QUEUE::OUTPUT)
        self.queues(QUEUE::AUDIT)
        self.queues(QUEUE::ERROR)
      end
    end
    @queues
  end

  def segmentCount
    @segmentCount = @segmentCount ? @segmentCount :
      (@jobspec[:segment_count] ? @jobspec[:segment_count] : 0).to_i
  end

  def segmentCount=(value)
    @segmentCount = @jobspec[:segment_count] = value
  end

  def update
   f = File.open(@jobspecFilename, "w+") { |f| f.write(@jobspec.to_yaml)  }
  end

    # Log messages to the lof file and stdout
  def log_message(log_msg_txt)
    `logger -t job specification #{log_msg_txt}`
    puts log_msg_txt
  end
end
=begin
js = JobSpec.new

puts js.codeFiles.length
puts js.codeFiles.first
puts js.r_function
=end