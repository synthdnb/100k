require 'oj'
require 'mysql2'
require 'sequel'
require 'pry'
require 'addressable/uri'
require 'htmlentities'

DB = Sequel.mysql2(database: :tweets, user: :root)

raw = DB[:tweets_raw]
twt = DB[:tweets]
url = DB[:urls]
hsh = DB[:hashtags]
DB.execute 'TRUNCATE TABLE tweets';
DB.execute 'TRUNCATE TABLE urls';
DB.execute 'TRUNCATE TABLE hashtags';
htmlentities = HTMLEntities.new
raw.order(:id).each do |tweet|
  begin
    obj = Oj.load(tweet[:raw_data])
    obj[:text] = htmlentities.decode(obj[:text])
    
    retweeted = false
    rt_obj = nil
    if obj[:retweeted_status]
      case obj[:retweeted_status]
      when Hash
        rt_obj = obj[:retweeted_status]
      when Array
        rt_obj = obj[:retweeted_status].select{|x| x[:user][:id] != obj[:user][:id]}.first
        raise unless rt_obj
      else
        raise
      end
      retweeted = true
    end
    if retweeted
      obj[:text].gsub!(/^RT @[^\s:]+: /,'')
    end
    extended_text = obj[:text]
    obj[:entities][:urls].each do |u|
      url.insert(tweets_id: obj[:id], url: u[:expanded_url], host: Addressable::URI.parse(u[:expanded_url]).host)
      extended_text = extended_text.gsub(u[:url], u[:expanded_url])
    end
    obj[:entities][:hashtags].each do |u|
      hsh.insert(tweets_id: obj[:id], tag: u[:text])
    end
    twt.insert(
      id: obj[:id],
      text: obj[:text],
      extended_text: extended_text,
      created_at: Time.parse(obj[:created_at]),
      source: obj[:source].gsub(/<[^>]+>/,''),
      in_reply_to_status_id: obj[:in_reply_to_status_id],
      in_reply_to_user_id: obj[:in_reply_to_user_id],
      in_reply_to_screen_name: obj[:in_reply_to_screen_name],
      retweet_count: obj[:retweet_count],
      favorite_count: obj[:favorite_count],
      retweeted: retweeted,
      favorited: obj[:favorited],
      user_id: retweeted ? rt_obj[:user][:id] : obj[:user][:id],
      user_name: retweeted ? rt_obj[:user][:screen_name] : obj[:user][:screen_name]
    )
  rescue => e
    binding.pry
    break
  end
end
