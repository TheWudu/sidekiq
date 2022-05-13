# frozen_string_literal: true

require "singleton"

module Sidekiq
  class Throttle
    include Singleton

    class << self
      def add(queue, concurrency)
        key = "queue:#{queue}"
        Sidekiq.redis { |c| c.hset("throttle:concurrencies", key, concurrency) }
      end

      def throttled_queues
        current_work = current_workset
        concurrencies.map do |key, limit|
          next if current_work[key].to_i < limit
          key
        end.compact
      end

      def running(queue)
        return unless concurrencies.key?(queue)
        Sidekiq.redis { |c| c.hincrby("throttle:currently_running", queue, 1) }
      end

      def done(queue)
        return unless concurrencies.key?(queue)
        Sidekiq.redis { |c| c.hincrby("throttle:currently_running", queue, -1) }
      end

      def run(job, &block)
        running(job.queue)
        block.call(job)
        done(job.queue)
      end

      def current_workset
        Sidekiq.redis { |c| c.hgetall("throttle:currently_running") }
      end

      def concurrencies
        Sidekiq.redis { |c| c.hgetall("throttle:concurrencies") }.transform_values!(&:to_i)
      end
    end
  end
end
