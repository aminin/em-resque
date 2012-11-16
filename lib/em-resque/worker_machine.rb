require 'em-synchrony'
require 'em-resque'
require 'em-resque/worker'

module EventMachine
  module Resque
    # WorkerMachine is an EventMachine with Resque workers wrapped in Ruby
    # fibers.
    #
    # An instance contains the workers and a system monitor running inside an
    # EventMachine. The monitoring takes care of stopping the machine when all
    # workers are shut down.

    class WorkerMachine
      include ::Resque::Helpers
      extend  ::Resque::Helpers
      # Initializes the machine, creates the fibers and workers, traps quit
      # signals and prunes dead workers
      #
      # == Options
      # fibers::       The number of fibers to use in the worker (default 1)
      # interval::     Time in seconds how often the workers check for new work
      #                (default 5)
      # queues::       Which queues to poll (default all)
      # verbose::      Verbose log output (default false)
      # vverbose::     Even more verbose log output (default false)
      # pidfile::      The file to save the process id number
      # tick_instead_of_sleep::      Whether to tick through the reactor polling for jobs or use EM::Synchrony.sleep.
      #                              Note that if you use this option, you'll be limited to 1 fiber.
      def initialize(opts = {})
        @interval = opts[:interval] || 5
        @fibers_count = opts[:fibers] || 1
        @queues = opts[:queue] || opts[:queues] || '*'
        @verbose = opts[:logging] || opts[:verbose] || false
        @very_verbose = opts[:vverbose] || false
        @pidfile = opts[:pidfile]
        @redis_namespace = opts[:namespace] || :resque
        @redis_uri = opts[:redis] || "redis://127.0.0.1:6379"

        @fibers = []

        raise(ArgumentError, "Should have at least one fiber") if @fibers_count.to_i < 1

        create_pidfile
      end

      # Start the machine and start polling queues.
      def start &block
        EM.synchrony do
          EM::Resque.initialize_redis(@redis_uri, @redis_namespace, @fibers_count)
          trap_signals
          prune_dead_workers

          queues = @queues.to_s.split(',')
          queues.each do |queue|
            dispatch_queue queue, &block
          end
        end
      end

      private

      def stop
        @shutdown = true
      end

      def dispatch_queue queue, &block
        work_loop = lambda do
          if @shutdown
            EM.stop
            next
          end

          if active_fibers_count < @fibers_count
            log! "Checking #{queue}"
            begin
              redis.lpop("queue:#{queue}").callback do |payload|
                if payload
                  job = ::Resque::Job.new(queue, decode(payload))
                  worker = spawn_worker
                  fiber = Fiber.new do
                    worker.log "starting async worker #{worker}"
                    worker.work_once job, &block
                  end
                  @fibers << fiber
                  fiber.resume
                  EM.next_tick &work_loop
                else
                  verbose_sleep @interval, "Waiting for #{queue}", &work_loop
                end
              end
            rescue Exception => e
              log "Error reserving job: #{e.inspect}"
              log e.backtrace.join("\n")
              raise e
            end
          else
            verbose_sleep @interval, "All workers are busy", &work_loop
          end
        end
        EM.next_tick &work_loop
      end

      def active_fibers_count
        @fibers.reject! { |f| !f.alive? }
        @fibers.count
      end

      def verbose_sleep(interval, message, &work_loop)
        log! "Sleeping for #{interval} seconds"
        procline message
        EM::Timer.new(interval) do
          EM.next_tick(&work_loop)
        end
      end

      def spawn_worker
        queues = @queues.to_s.split(',')
        worker = EM::Resque::Worker.new(*queues)
        worker.verbose = @verbose
        worker.very_verbose = @very_verbose
        worker
      end

      def procline(string)
        $0 = "resque-#{Resque::Version}: #{string}"
        log! $0
      end

      # Traps signals TERM, INT and QUIT to stop the machine.
      def trap_signals
        ['TERM', 'INT', 'QUIT'].each { |signal| trap(signal) { stop } }
      end

      # Deletes worker information from Redis if there's now processes for
      # their pids.
      def prune_dead_workers
        redis.smembers(:workers).callback do |all_workers|
          known_workers = worker_pids unless all_workers.empty?
          all_workers.each do |worker_id|
            host, pid, queues, fiber_id = worker_id.split(':')
            next unless host == hostname
            next if known_workers.include?(pid)
            log! "Pruning dead worker: #{worker_id}"
            queues = queues.split(',')
            worker = EM::Resque::Worker.new(*queues)
            worker.to_s = worker_id
            worker.unregister_worker
          end
        end
      end

      def hostname
        @hostname ||= `hostname`.chomp
      end

      def worker_pids
        `ps -A -o pid,command | grep "[r]esque" | grep -v "resque-web"`.split("\n").map do |line|
          line.split(' ')[0]
        end
      end

      def create_pidfile
        File.open(@pidfile, 'w') { |f| f << Process.pid } if @pidfile
      end

      # Log a message to STDOUT if we are verbose or very_verbose.
      def log(message)
        if @verbose
          puts "*** #{message}"
        elsif @very_verbose
          time = Time.now.strftime('%H:%M:%S %Y-%m-%d')
          puts "** [#{time}] #$$: #{message}"
        end
      end

      # Logs a very verbose message to STDOUT.
      def log!(message)
        log message if @very_verbose
      end
    end
  end
end
