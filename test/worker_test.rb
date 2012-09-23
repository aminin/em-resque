require 'test_helper'
require 'em-resque/worker'

context "Worker" do
  setup do
    EM::Resque.redis.flushall
  end

  test "processes jobs" do
    EM.synchrony do
      EM::Resque.enqueue(TestJob, 420, 'foo')
      worker = EM::Resque::Worker.new('*')
      worker.work(0)

      EM::Timer.new(0.5) do
        assert_equal 1, EM::Resque.info[:processed]

        worker.shutdown!
        EM.stop
      end
    end
  end

  test "logs the processed queue" do
    EM.synchrony do
      EM::Resque.enqueue(TestJob, 420, 'test processed')
      worker = EM::Resque::Worker.new('*')
      worker.work(0)

      EM::Timer.new(0.5) do
        assert_equal 1, EM::Resque.redis.get("stat:processed_jobs").to_i

        worker.shutdown!
        EM.stop
      end
    end
  end

  test "fails bad jobs" do
    EM.synchrony do
      EM::Resque.enqueue(FailJob, 420, "foo")
      worker = EM::Resque::Worker.new('*')
      worker.work(0)

      EM::Timer.new(0.5) do
        assert_equal 1, Resque::Failure.count
        worker.shutdown!
        EM.stop
      end
    end
  end
end
