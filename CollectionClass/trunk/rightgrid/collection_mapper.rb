require 'yaml'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
require 'optparse'
require 'pp'
require './collection_dispatcher.rb'
require './collection_collector.rb'
	##################################################################################
	#
	# The logic for this demo program is as follows:
	#
	#     1) Read the local directory for .jpgs
	#     2) Upload data to an S3 bucket/key
	#	  3) Construct a work_unit
	#	  4) Encode the work_unit with YAML to a message
	#	  5) Enqueue the message
	#
	##################################################################################
	options = {}
	optparse = OptionParser.new do |opts|
		# Set a banner, displayed at the top of the help screen.
		opts.banner = "Usage: job_mapper.rb -q queue_type(audit,error,output) -t sleeptime -x ..."


  opts.on('-h', '--help', '-js --jobspec <jobspec.yml>') do
    puts opts
    exit
  end
  options[:jobspec] = ""
  opts.on( '-js', '--jobspec', 'job specification file name' ) do |wname|
    options[:jobspec] = wname
  end

  options[:dispatched] = 0
  opts.on( '-z', '--dispatched NUM', 'number of input queue tasks dispatched' ) do |tsk|
    options[:dispatched] = tsk.to_i
  end

  options[:r_array_size] = 0
  opts.on( '-r', '--r_array_size NUM', 'size of array to allocate' ) do |tsk|
    options[:r_array_size] = tsk.to_i
  end

  options[:r_postprocess] = ""
  opts.on( '-q', '--r_postprocess name', 'post processing command for collection' ) do |tsk|
    options[:r_postprocess] = tsk.to_s
  end

  options[:r_function] = ""
  opts.on( '-f', '--r_function name', 'lapply function command for collection' ) do |tsk|
    options[:r_function] = tsk.to_s
  end

  options[:r_preamble] = ""
  opts.on( '-p', '--r_preamble name', 'preamble for lapply function command for collection' ) do |tsk|
    options[:r_preamble] = tsk.to_s
  end

  options[:workername] = ""
  opts.on( '-w', '--worker class name', 'name of worker class' ) do |wname|
    options[:workername] = wname
  end

  options[:audit] = false
    opts.on( '-a', '--audit', 'pop all messages off queue' ) do
    options[:audit] = true
  end

end
begin
	optparse.parse!
	rescue Exception => e
  STDERR.puts e
  STDERR.puts optparse
  exit(-1)
end
=begin
=end


CollectionDispatcher.new
#sleep(2)
CollectionCollector.new
=begin
=end