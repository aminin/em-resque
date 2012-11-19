require 'test_helper'
require 'em-resque/worker_machine'

context 'WorkerMachine' do
  test 'should initialize itself' do
    machine = EM::Resque::WorkerMachine.new

    assert_equal 1, machine.fibers.count
    assert_equal 1, machine.workers.count
    assert_equal Fiber, machine.fibers[machine.workers.first.object_id].class
    assert_equal EM::Resque::Worker, machine.workers.first.class
  end

  test 'should not run with under one fibers' do
    assert_raise(ArgumentError, "Should have at least one fiber") do
      machine = EM::Resque::WorkerMachine.new :fibers => 0
    end
  end
end
