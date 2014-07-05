require 'pry'
require 'csv'
require 'twitter'
require 'sequel'
require 'mysql2'
require 'oj'
require 'highline/import'

twitter = Twitter::REST::Client.new do |config|
  #your-twitter-api-config
end

DB = Sequel.mysql2(user: 'devenv', password: ask("DB password? : "){ |q| q.echo = false }, database: 'user_devenv_tweet')

last_id = nil
total_sleep = 0
id_list = []

CSV.foreach('tweets.csv') do |twt|
  unless last_id
    last_id = twt[0]
    next
  end
  id_list << twt[0]
end

id_list.each_slice(100) do |tweets|
  begin
    statuses = twitter.statuses(*tweets, map: true)
  rescue => e
    binding.pry
    puts e.message
    puts e.backtrace.join("\n")
    puts "#{DB[:tweets_raw].count} failed"
    
    t_sleep = nil
    if total_sleep < 60*15
      t_sleep = 60*15+30
    else
      t_sleep = 60
    end
    total_sleep += t_sleep
    puts "#{Time.now} sleep for #{t_sleep/60.0} minutes"
    sleep(t_sleep)
    retry
  end
  total_sleep = 0
  statuses.each do |status|
    if DB[:tweets_raw].where(id: status.id).update(id: status.id, raw_data: Oj.dump(status.to_h)) == 0
      DB[:tweets_raw].insert(id: status.id, raw_data: Oj.dump(status.to_h))
      puts "Inserted #{status.id}"
    else
      puts "Updated #{status.id}"
    end
  end
end
