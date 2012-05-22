require 'json'
require "net/http"
require "uri"

ENV["HOST"] ||= "garcia"

uri = URI.parse("http://localhost:5000/version?host=#{ENV["HOST"]}&port=8020&username=pivotal")

# Shortcut
response = Net::HTTP.get_response(uri)

if !response.body.empty?
    puts "Version OK: #{response.body}" 
else
    puts "Version NOT OK"
end

uri = URI.parse("http://localhost:5000/0.20.1gp/list/%2Fwebui%2F?host=gillette&port=8020&username=pivotal")
response = Net::HTTP.get_response(uri)
entries = JSON.parse(response.body)

if entries.size > 0
    puts "Listing OK: #{entries.size} items"
else
    puts "Listing NOT OK"
end
