class CacheWorker
  def initialize(opts)
    @log = opts['log']

    @zk_helper = ZkHelper.new(opts, self)
    @cache = Cache.new(opts)
    @opts = opts['fetch_interval']
  end

  def start
    reload_instances
    run
  end

  def stop
    @log.warn "stopping cache worker"
    @zk_helper.close
    @cache.close
  end

  def reload_instances
    instances = @zk_helper.load_instances_from_zk.freeze
    @cache.set_results(instances)
  end

  def run
    while true
      begin
        begin
          sleep(@fetch_interval)
        rescue
          nil
        end
        @log.info "Cache fetch thread now fetches from zk..."
        begin
          reload_instances
        rescue
          nil
        end
        throw_if_stale
      rescue => ex
        @log.warn "Caught exception in cache fetch thread: #{ex} #{ex.backtrace}"
      end
    end
  end
end
