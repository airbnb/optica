
require 'chef/handler'
require 'net/http'
require 'fileutils'
require 'json'
require 'tempfile'

module Optica
  class Reporter < Chef::Handler

    MAX_TRIES = 4
    SAVED_REPORTS_DIR = '/tmp/failed_optica_reports'
    SAVED_REPORTS_PREFIX = 'optica_report'

    def report
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
      data['branch']           = safe_node_attr(['branch'])
      data['roles']            = safe_node_attr(['roles'])
      data['recipes']          = safe_node_attr(['recipes'])
      data['synapse_services'] = safe_node_attr(['synapse', 'enabled_services'])
      data['nerve_services']   = safe_node_attr(['nerve', 'enabled_services'])
      data['ownership']        = safe_node_attr(['ownership'])

      # ip is needed by optica for all reports
      data['ip']                 = safe_node_attr(['ipaddress'])

      data['environment']        = safe_node_attr(['env'])
      data['role']               = safe_node_attr(['role'])
      data['id']                 = safe_node_attr(['ec2', 'instance_id'])
      data['hostname']           = safe_node_attr(['hostname'])
      data['uptime']             = safe_node_attr(['uptime_seconds'])
      data['public_hostname']    = safe_node_attr(['ec2', 'public_hostname'])
      data['public_ip']          = safe_node_attr(['ec2', 'public_ipv4'])
      data['az']                 = safe_node_attr(['ec2', 'placement_availability_zone'])
      data['security_groups']    = safe_node_attr(['ec2', 'security_groups'])
      data['instance_type']      = safe_node_attr(['ec2', 'instance_type'])
      data['ami_id']             = safe_node_attr(['ec2', 'ami_id'])
      data['intended_branch']    = File.read('/etc/chef/branch').strip

      converge_reason = ENV['CONVERGE_REASON']
      data['converge_reason']  = converge_reason unless converge_reason.nil?

      converger = ENV['IDENTITY']
      data['converger'] = converger unless converger.nil?

      # report the data
      Chef::Log.info "Sending run data to optica"
      tries = 0
      begin
        tries += 1

        connection = Net::HTTP.new('optica.d.musta.ch', 443)
        connection.use_ssl = true
        result = connection.post('/', data.to_json)

        if result.code.to_i >= 200 and result.code.to_i < 300
          Chef::Log.info "SUCCESS: optica replied: '#{result.body}'"
        else
          raise StandardError.new("optica replied with: #{result.code}:#{result.body}")
        end
      rescue => e
        if tries < MAX_TRIES
          Chef::Log.info "FAILED: error reporting to optica from #{data['hostname']} (#{e.message}); trying again..."
          sleep 2 ** tries
          retry
        end

        Chef::Log.error "FAILED: error reporting to optica from #{data['hostname']}: #{e.message} #{e.backtrace}"
        save_report(data)
      else
        delete_old_reports
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

    def save_report(data)
      FileUtils.mkdir_p(SAVED_REPORTS_DIR)

      filename = File.join(SAVED_REPORTS_DIR, SAVED_REPORTS_PREFIX + Time.now.to_i.to_s)
      File.open(filename, 'w') do |sr|
        sr.write(data.to_json)
      end

      Chef::Log.info "Optica backup report written to #{filename}"
    end

    def delete_old_reports
      to_delete = Dir.glob(File.join(SAVED_REPORTS_DIR, SAVED_REPORTS_PREFIX + "*"))
      unless to_delete.length == 0
        Chef::Log.info "Deleting #{to_delete.length} unsent optica reports"
        to_delete.each { |old_report| File.delete(old_report) }
      end
    end
  end
end
