class TaskHelper
  def self.parse_opts_from_env
    integer_keys = %w(CONCURRENCY INTERVAL FIBERS)
    string_keys = %w(QUEUE QUEUES PIDFILE)
    bool_keys = %w(LOGGING VERBOSE VVERBOSE TICK_INSTEAD_OF_SLEEP)

    ENV.reduce({}) do |acc, (k, v)|
      acc = acc.merge(k.downcase.to_sym => v.to_i) if integer_keys.any?{|ik| ik == k}
      acc = acc.merge(k.downcase.to_sym => v.to_s) if string_keys.any?{|sk| sk == k}
      acc = acc.merge(k.downcase.to_sym => v == '1' || v.downcase == 'true') if bool_keys.any?{|bk| bk == k}
      acc
    end
  end
end
