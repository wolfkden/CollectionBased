require 'yaml'
require 'rubygems'
require 'right_aws'
require 'uri'
require 'net/smtp'
require 'stringio'
require './job_spec.rb'

class Machine
  attr_reader :machine_id, :session_list, :dispatched, :processed, :parent

  def initialize(session_list, jobSpec=nil, parent=nil)
   super()

   @machine_id = session_list.instance_of?(Hash) ?
      (@session_list = session_list).keys.first : session_list

   @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
   return if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec)

    @parent = parent
  end

  def parent
    @parent
  end

  def base_directory
    [".", machine_id.to_s].join("/")
  end

  def sessions
    session_list ? session_keys.map { |k| session(k) } : nil
  end

  def session_keys
    session_list ? session_list[machine_id].keys : nil
  end

  def session(session_id)
    Session.new(collection_list(session_id), @jobSpec, self)
  end

  def collection_list(session_id)
    { session_id => session_list[machine_id][session_id] }
  end

  def session_list
    @session_list.instance_of?(Hash) ? @session_list :
      { @machine_id => dispatched[dispatched.keys.first] }
  end

  def machine_id
    @machine_id ? @machine_id :
      (@machine_id = dispatched.keys.first)
  end

  def dispatched
    @dispatched = @dispatched ? @dispatched : YAML::load_file(@jobSpec.taskDispatchList)
    @dispatched = @dispatched.instance_of?(Hash) ? @dispatched : {}
  end

  def processed
    @processed = @processed ? @processed : YAML::load_file(@jobSpec.taskProcessedList)
    @processed = @processed.instance_of?(Hash) ? @processed : {}
  end

  def delete(session)
    session = session.session_id if session.instance_of? Session
    session = session.to_sym if session.instance_of? String

    return nil if !session.instance_of? Symbol

    @dispatched[machine_id].delete(session)
    session = nil
  end

  def update(collection_list=nil)

    dispatched[machine_id][collection_list.keys.first] =
      collection_list.first.last if !collection_list.nil? &&
      collection_list.instance_of?(Hash) && 0 < collection_list.length

    puts "updating machine"
    File.open(@jobSpec.taskDispatchList, "w+") { |f| f.write(dispatched.to_yaml) }

  end

  def to_hash
    session_list
  end
end

class Session
  attr_reader :session_id, :collection_list, :dispatched, :parent, :base_directory
  def initialize(collection_list, jobSpec=nil, parent=nil)
    super()

    @session_id = collection_list.instance_of?(Hash) ?
      (@collection_list = collection_list).keys.first :
      collection_list

    @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
    return if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec)

    @parent = parent.instance_of?(Machine) ? parent : nil

  end

  def parent
    @parent
  end

  def machine
    @parent
  end

  def base_directory
    return @base_directory if !@base_directory.nil?
    @base_directory =
      [(parent.nil? ? "." : parent.base_directory), @session_id].join("/")
  end

  def session_id
    @session_id ? @session_id :
    (@sesion_id = collection_list.keys.first)
  end

  def collections
    collection_keys.map { |k| collection(k) }
  end

  def collection(collection_id)
    Collection.new(task_list(collection_id), @jobSpec, self)
  end

  def task_list(collection_id)
    { collection_id => collection_list[@session_id][collection_id] }
  end

  def collection_keys
    collection_list[session_id].keys
  end

  def collection_list
    @collection_list.instance_of?(Hash) ? @collection_list
    : (@collection_list = { @session_id => dispatched[dispatched.keys.first][@session_id] })
  end

  def dispatched
    @dispatched = @dispatched ? @dispatched : YAML::load_file(@jobSpec.taskDispatchList)
    @dispatched = @dispatched.instance_of?(Hash) ? @dispatched : {}
  end

  def machine_id
    [!machine.nil? ? [machine.machine_id] :
      dispatched.select { |k, v|  0 < v.select { |u,w| u == session_id }.length }
        .map { |k,v| k }].first
  end

  def delete(collection)
    collection = collection.collection_id if collection.instance_of? Collection
    collection = collection.to_sym if collection.instance_of? String

    return nil if !collection.instance_of? Symbol

    collection_list[session_id].delete(collection)

    return nil if machine_id.nil?

    dispatched[machine_id][session_id].delete(collection)
  end

  def update(task_list=nil)

    collection_list[session_id][task_list.keys.first] =
      task_list.first.last if !task_list.nil? &&
        task_list.instance_of?(Hash) && 0 < task_list.length

    return machine.update(collection_list) if !machine.nil?

    dispatched[machine_id][session_id] = collection_list.first.last

    File.open(@jobSpec.taskDispatchList, "w+") { |f| f.write(dispatched.to_yaml) }

  end

  def to_hash
    collection_list
  end
end

class Collection
  attr_reader :collection_id, :task_list, :base_directory

  def initialize(task_list, jobSpec = nil, parent=nil)
    super()

    return if !task_list.instance_of?(Hash) || task_list.length < 1
    task_list[task_list.keys.first] = {} if !task_list[task_list.keys.first].instance_of?(Hash)

    @collection_id = task_list.instance_of?(Hash) ?
      (@task_list = task_list).keys.first : nil

    @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
    if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec) then return end

    @parent = parent.instance_of?(Session) ? parent : nil

  end

  def parent
    @parent
  end

  def session
    @parent
  end

  def base_directory
    return @base_directory if !@base_directory.nil?
    (@base_directory = [(parent.nil? ? "." : parent.base_directory), @collection_id].join("/"))
  end

  def collection_id
    @collection_id
  end

  def tasks
    task_list[collection_id].keys.map { |k| task(k) }
  end

  def task(task_id)
    Task.new(attribute_list(task_id), @jobSpec, self)
  end

  def attribute_list(task_id)
    { task_id => task_list[collection_id][task_id] }
  end

  def task_keys
    task_list[collection_id].keys
  end

  def task_sequence
    return nil if task_keys.nil?
    base = task_keys.first.to_s.split(/_/).first
    task_keys.map { |k| k.to_s.split(/_/).last.to_i }
      .sort.map { |i| "#{base}_#{i}" }
  end

  def task_list
    @task_list ? @task_list : { collection_id=> nil }
  end

  def deleteTasks
    tasks.each { |t| delete(t) }
  end

  def delete(task)
    task = task.task_id if task.instance_of? Task
    task = task.to_sym if task.instance_of? String

    return nil if !task.instance_of? Symbol

    task_list[collection_id].delete(task)

  end

  def update(attribute_list=nil)

    task_list[collection_id][attribute_list.keys.first] =
      attribute_list.first.last if !attribute_list.nil? &&
        attribute_list.instance_of?(Hash) && 0 < attribute_list.length

    session.update(task_list) if !session.nil?
  end

  def to_hash
    task_list
  end
end

class Task
  attr_reader :task_id, :attribute_list, :parent, :base_directory

  def initialize(attribute_list, jobSpec=nil, parent=nil)
    @task_id = attribute_list.instance_of?(Hash) ?
      (@attribute_list = attribute_list).keys.first : nil

    @jobSpec = jobSpec == nil || !(jobSpec.is_a? JobSpec) ? JobSpec.new : jobSpec;
    if @jobSpec  == nil || !(@jobSpec .is_a? JobSpec) then return end

    @parent = parent.instance_of?(Collection) ? parent : nil

  end

  def parent
    @parent
  end

  def collection
    @parent
  end

  def base_directory
    @base_directory ? @base_directory :
      (@base_directory = parent.nil? ? "." : parent.base_directory)
  end

  def base_identifier
    task_id ? task_id.to_s.split(/_/).first : nil
  end

  def task_id
    @task_id
  end

  def task_files
    !(al = attribute_list) || !(al = al[task_id]) ? [] :
      [upload_files, s3_files].flatten.select { |f| !f.nil? }
  end

  def s3_files
    !(al = attribute_list) || !(al = al[task_id]) ? [] :
      [al["collectionS3"]].flatten.select { |f| !f.nil? }
  end

  def upload_files
    !(al = attribute_list) || !(al = al[task_id]) ? [] :
      [al["collectionFunction"], al["collectionData"]]
        .flatten.select { |f| !f.nil? }
          .map { |f| [base_directory, f].join("/") }
  end

  def data_files
    !(al = attribute_list) || !(al = al[task_id]) ? [] :
      [al["collectionData"]].flatten.select { |f| !f.nil? && 0 < f.length }
        .map { |f| [base_directory, f].join("/") }
  end

  def function_files
    !(al = attribute_list) || !(al = attribute_list[task_id]) ? [] :
      [al["collectionFunction"]].flatten.select { |f| !f.nil? }
        .map { |f| [base_directory, f].join("/") }
  end

  def attribute_keys
    attribute_list ? attribute_list[task_id].keys : nil
  end

  def attribute_list
    @attribute_list ? @attribute_list : nil
  end

  def update
    parent.update(attribute_list) if !collection.nil?
  end

  def to_hash
    attribute_list
  end
end
