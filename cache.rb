require 'oj'
require 'hammerspace'
require 'time'

class Cache

  DEFAULT_CACHE_STALE_AGE = 0
  H_DIR = "/tmp/hammerspace".freeze
  H_UPDATE_TIME_KEY = "update_time".freeze
  H_RESULTS_KEY = "results".freeze

  def initialize(opts)
    @log = opts['log']

    @cache_stale_age = opts['cache_stale_age'] || DEFAULT_CACHE_STALE_AGE

    # concurrency safe cache
    @h = Hammerspace.new(H_DIR)
    @h[H_UPDATE_TIME_KEY] = Time.now.to_s
  end

  def get_results
    Oj.load(@h[H_RESULTS_KEY])
  end

  def set_results(results)
    # Hammerspace is threadsafe and latest write "wins"
    # so no need to guard with mutex
    @h[H_UPDATE_TIME_KEY] = Time.now.to_s
    @h[H_RESULTS_KEY] = Oj.dump(results)
    flush_cache
  end

  def flush_cache
    @h.close
    @h = Hammerspace.new(H_DIR)
  end

  def throw_if_stale
    cache_age = get_cache_age_secs

    if @cache_stale_age > 0 && cache_age > @cache_stale_age
      msg = "cache age exceeds threshold: #{cache_age} > #{@cache_stale_age}"

      @log.error msg
      raise msg
    end
  end

  def get_cache_age_secs
    last_fetched_time = Time.parse(@h[H_UPDATE_TIME_KEY])
    cache_age_secs = Time.new.to_i - last_fetched_time.to_i

    STATSD.gauge 'optica.store.cache.age', cache_age_secs
  end

  def close
    @log.warn "closing cache"
    @h.close
  end
end

