require 'json'
require "net/http"
require "uri"

ENV["HOST"] ||= "garcia"

uri = URI.parse("http://localhost:5000/hello-world?host=#{ENV["HOST"]}&port=8020&username=pivotal")

# Shortcut
response = Net::HTTP.get_response(uri)
puts response.body

exit response.body.empty? ? 1 : 0
