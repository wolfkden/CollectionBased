require '/var/spool/ec2/meta-data-cache.rb'

class RGKicker


	def do_work(message_env, message)

		# Debug Message
		puts message_env.inspect

		starttime = Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC')

		## Best Practice - Test all variables before use
		# check to make sure all variables have been defined
		var_check = 0
		begin
			s3_downloaded_list = message_env[:s3_downloaded_list]
#      r_machine_id = message_env[:r_machine_id]
#      r_session_id = message_env[:r_session_id]
#      r_collection_id = message_env[:r_collection_id]
			odir = message_env[:output_dir]
			serial = message_env[:serial]
			ec2_id = "#{ENV['EC2_INSTANCE_ID']}"
			wname = message_env[:worker_name]
			messageid = message_env[:message_id]
			# since worker has no access to s3_out, we pass it via user: section in .yml
			user_s3 = message_env[:user][:s3_out]
			m_s3_in = message_env[:s3_in]
			m_s3_out = message_env[:s3_out]
			var_check = 1
		rescue
			puts "An expected variable is not defined"
		end

		if var_check then
			# Get the input path/filename
      function_list = []
      data_list = []
			# Get the input path/filename
      s3_downloaded_list.keys.each do |key|
        item = s3_downloaded_list[key].gsub(/\(/, '\(').gsub(/\)/, '\)')
        data_list.push(item) if /data\.file/i.match(item)
        function_list.push(item) if /function\.file/i.match(item)
      end

			# Construct the output path/filename

      input_path_file = data_list.first

      cmd_data =
        "aa<-if(is.function(clonedWorker@collection[[1]])) lapply(clonedWorker@collection[[1]](), eval(as.name(names(clonedWorker@func)[1]))) else lapply(scan('#{input_path_file}'), eval(as.name(names(clonedWorker@func)[1])));unlink('#{input_path_file}');"

      function_file = function_list.length ?
        "load('#{function_list.first}');writeTree(clonedWorker@func);#{cmd_data}" : ""

#  "#{s3_downloaded_list[s3_downloaded_list.keys.first].gsub(/\(/, '\(').gsub(/\)/, '\)')}"

			# Construct the output path/filename
			input_file = File.basename("#{input_path_file}")
			output_path_file = "#{odir}/#{input_file}"

			# DEBUG report in the logfile the conversion to be performed
			puts "/usr/bin/R -e \"#{function_file}write(unlist(aa), '#{output_path_file}')\""
			puts "input_file: #{input_file}, outputdir = #{odir} s3_in: #{m_s3_in} s3_out: #{m_s3_out}"
#     puts "access key: #{access_key} secret access_key: #{secret_access_key}"
			# execute and place output to stdout
			r_execution = `/usr/bin/R -e \"#{function_file}write(unlist(aa), '#{output_path_file}');write(paste('collection length: ', length(unlist(aa)), ''), stdout())\"`

      puts r_execution
			# Get the results of the conversion
			if $?.exitstatus == 0
				rg_result = 0
				puts "Class: #{wname} Serialid=#{serial} successfully processed on worker instance #{ec2_id}"
			else
				# if the :result element passed to the worker daemon begins with exception or aborted, then the daemon
				# will handle it as a permanent error
				rg_result = "exception: #{wname} R colection ended with #{$?.exitstatus} status: serialID=#{serial} instance: #{ec2_id}"
				puts rg_result
			end
		end

		s3_construct = "#{user_s3}/#{messageid}"
		next_work_file = File.join("#{s3_construct}", "#{input_file}")

		finishtime = Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC')

		# Populate the result structure to return back to the daemon
		result = {
			:result => rg_result,
			:id => message_env[:id],
			:starttime => starttime,
			:finishtime => finishtime,
			:serial => serial,
			:audit_info => {
				:serial => serial,
				:ec2_instance_id => ec2_id
			},
      :collection_size => r_execution.split('collection length: ').last.to_i,
			### OPTIONAL SECTION  ### CHAINED RESULTS  ###  CREATE WORK_UNIT ###
			# If you want this output to be the input to another worker array, then
			# create another work_unit for the next GridWorker (chained Results) to process
			:s3_download => [next_work_file],
			:created_at =>  Time.now.utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
			# if worker_name omitted, then next worker will use its default_worker_name
			:worker_name => "RGWatermark"
		}
	end

end