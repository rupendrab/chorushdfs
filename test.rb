require 'json'
require "net/http"
require "uri"

ENV["HOST"] ||= "garcia"

uri = URI.parse("http://localhost:5000/version?host=#{ENV["HOST"]}&port=8020&username=pivotal")

# Shortcut
response = Net::HTTP.get_response(uri)

if !response.body.empty? && response.code == "200"
    puts "Version OK: #{response.body}" 
else
    puts "Version NOT OK"
end

uri = URI.parse("http://localhost:5000/1.0.0/list/%2F?host=garcia&port=8020&username=pivotal")
response = Net::HTTP.get_response(uri)
entries = JSON.parse(response.body)

if entries.size > 0 && response.code == "200"
    puts "Listing OK: #{entries.size} items"
else
    puts "Listing NOT OK"
end

uri = URI.parse("http://localhost:5000/1.0.0/show/%2Ffiletypes%2FAccountMapHelper.java?host=garcia&port=8020&username=pivotal")
response = Net::HTTP.get_response(uri)

file = response.body
if file.size > 0 && response.code == "200"
    puts "Content OK: #{file.size} bytes"
else
    puts "Content NOT OK"
end
