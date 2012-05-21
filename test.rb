require 'json'
require "net/http"
require "uri"

uri = URI.parse("http://localhost:5000/hello-world")

# Shortcut
response = Net::HTTP.get_response(uri)
entries = JSON.parse(response.body)

puts entries.size > 0
exit entries.empty? ? 1 : 0
