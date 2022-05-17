# frozen_string_literal: true

require "singleton"

module Sidekiq
  class Throttle
    class << self
      EXPIRE_TTL = 600

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
        key = current_key(queue)
        redis.incrby(key, 1)
        redis.expire(key, EXPIRE_TTL)
      end

      def done(queue)
        return unless concurrencies.key?(queue)
        key = current_key(queue)
        redis.incrby(key, -1)
        redis.expire(key, EXPIRE_TTL)
      end

      def run(job, &block)
        running(job.queue)
        block.call(job)
      ensure
        done(job.queue)
      end

      def current_key(queue)
        "throttle:current:#{queue}"
      end

      def current_workset
        redis.keys("throttle:current:*").each_with_object({}) do |key, h|
          queue = key.gsub("throttle:current:","")
          h[queue] = redis.get(key)
        end
      end

      def concurrencies
        Sidekiq.redis { |c| c.hgetall("throttle:concurrencies") }.transform_values!(&:to_i)
      end

      def redis
        Sidekiq.redis { |c| c }
      end
    end
  end
end
