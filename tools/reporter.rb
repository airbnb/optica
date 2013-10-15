
require 'chef/handler'
require 'net/http'
require 'json'

module Optica
  class Reporter < Chef::Handler

    def report
      optica_server = 'optica.example.com'
      data = {}

      # include self-reported attributes if present
      report = safe_node_attr(['optica', 'report'])
      report.each do |k, v|
        data[k] = v
      end if report

      # include run info
      data['failed'] = run_status.failed?
      data['last_start'] = run_status.start_time
      data['last_reported'] = Time.now

      # include some node data (but fail gracefully)
      data['hostname']         = safe_node_attr(['hostname'])
      data['environment']      = safe_node_attr(['env'])
      data['role']             = safe_node_attr(['role'])
      data['roles']            = safe_node_attr(['roles'])
      data['recipes']          = safe_node_attr(['recipes'])
      data['synapse_services'] = safe_node_attr(['synapse', 'enabled_services'])

      # report the data
      Chef::Log.info "Sending run data to optica"
      tries = 0
      begin
        connection = Net::HTTP.new(optica_server, 8080)
        result = connection.post('/', data.to_json)

        if result.code.to_i >= 200 and result.code.to_i < 300
          Chef::Log.info "SUCCESS: optica replied: '#{result.body}'"
        else
          Chef::Log.error "FAILED: optica replied wiith: #{result.code}:#{result.body}"
        end
      rescue => e
        if tries <= 3
          tries += 1
          Chef::Log.info "FAILED: error reporting to optica from #{data['hostname']} (#{e.message}); trying again..."
          sleep 2 ** tries
          retry
        else
          Chef::Log.error "FAILED: error reporting to optica from #{data['hostname']}: #{e.message} #{e.backtrace}"
        end
      end
    end

    def safe_node_attr(attr_list)
      return nil unless attr_list.is_a? Array

      obj = run_status.node
      processed = ['node']

      attr_list.each do |a|
        begin
          obj = obj[a]
        rescue
          Chef::Log.info "Failed to get attribute #{a} from #{processed.join('.')}"
          return nil
        else
          processed << a
        end
      end

      return obj
    end
  end
end
