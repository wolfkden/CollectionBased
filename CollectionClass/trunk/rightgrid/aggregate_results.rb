require 'yaml'
require 'pathname'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
#require '/var/spool/ec2/meta-data-cache.rb'
#require '/var/spool/ec2/user-data.rb'
#require 'optparse'
require './job_spec.rb'

class AggregateResults
  attr_accessor :baseName, :outputDir, :collection

  def initialize(jobSpec = nil, baseName = nil)

    @outputDir = "output"
    jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;

    @collection = []
    if baseName == nil && jobSpec.uploadedFiles
    jobSpec.uploadedFiles.each { |file|
      bfile = Pathname.new(file).basename.to_s
      @collection.push(AggregateResults.new(jobSpec, bfile))
    }
    else
      @baseName = baseName
      self.aggregateFiles
    end

  end

  def aggregateFiles
    Dir.chdir(@outputDir)
  	datafiles = Dir.glob(@baseName + '_*')

    (aa = @baseName.split("_")).shift
    dupFile = File.open(aa.join("_") + "_output", File::RDWR|File::TRUNC|File::CREAT, 0666)
    aggFile = File.open(@baseName + "_output", File::RDWR|File::TRUNC|File::CREAT, 0666)
    counter =  datafiles.select{|x| x.scan(/(_output)$/).length == 0}.map{|x| x.scan(/\d+/).last.to_i }.sort
    counter.each { |cnt|
      file = File.open(@baseName + "_" + cnt.to_s, "r").each_line { |line|
        aggFile.puts line;
        dupFile.puts line;
      }
      file.close
    }
    aggFile.close
  end
end

AggregateResults.new