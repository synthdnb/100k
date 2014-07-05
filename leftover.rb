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

DB = Sequel.mysql2(database: :tweets, user: :root)

options = {:count => 200, :include_rts => true}
statuses = twitter.user_timeline("iteratorP", options)
statuses.each do |status|
  if DB[:tweets_raw].where(id: status.id).update(id: status.id, raw_data: Oj.dump(status.to_h)) == 0
    DB[:tweets_raw].insert(id: status.id, raw_data: Oj.dump(status.to_h))
    puts "Inserted #{status.id}"
  else
    puts "Updated #{status.id}"
  end
end
