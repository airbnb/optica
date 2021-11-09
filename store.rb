class Store
  attr_reader :ips

  def initialize(opts)
    @log = opts['log']

    if !!opts['cache_enabled']
      @cache = Cache.new(opts, nil)
    else
      @zk_helper = ZkHelper.new(opts)
    end
  end

  def stop
    @log.warn "stopping the store"
    @zk_helper.close if @zk_helper
  end

  # get instances for a given service
  def nodes()
    STATSD.time('optica.store.get_nodes') do
      return @zk_helper.load_instances_from_zk unless @cache

      check_cache_age
      @cache.get_results
    end
  end

  def healthy?()
    @zk_helper.healthy?
  end
end
