require 'sinatra/base'
require 'json'
require 'cgi'

class Optica < Sinatra::Base
  configure :production, :development do
    enable :logging
  end

  get '/' do
    params = CGI::parse(request.query_string)

    # include only those nodes that match passed-in parameters
    examined = 0
    to_return = {}
    settings.store.nodes.each do |node, properties|
      examined += 1
      included = true

      params.each do |param, values|
        values.each do |value|
          if not properties.include? param
            included = false
          elsif properties[param].is_a? String
            included = false unless properties[param].match value
          elsif properties[param].is_a? Array
            included = false unless properties[param].include? value
          elsif properties[param].class == TrueClass
            included = false unless ['true', 'True', '1'].include? value
          elsif properties[param].class == FalseClass
            included = false unless ['false', 'False', '0'].include? value
          end
        end
      end

      to_return[node] = properties if included
    end

    content_type 'application/json', :charset => 'utf-8'
    result = {'examined'=>examined, 'returned'=>to_return.count, 'nodes'=>to_return}
    return result.to_json
  end

  post '/' do
    begin
      data = JSON.parse request.body.read
    rescue JSON::ParserError
      data = {}
    end

    # the node ip is in the request
    ip = request.ip

    halt(403) unless settings.store.ips.include? ip

    settings.store.add(ip, data)
    settings.events.send(data)

    content_type 'text/plain', :charset => 'utf-8'
    return 'stored'
  end

  delete '/:hostname' do |hostname|
    matching = settings.store.nodes.select{ |k,v| v['hostname'] == hostname }
    if matching.length == 0
      return 204
    elsif matching.length == 1
      settings.store.delete(matching[0]['ip'])
      return "deleted"
    else
      return [409, "found multiple entries matching hostname #{hostname}"]
    end
  end

  get '/health' do
    if settings.store.healthy? and settings.events.healthy?
      content_type 'text/plain', :charset => 'utf-8'
      return "OK"
    else
      halt(503)
    end
  end

  get '/ping' do
    content_type 'text/plain', :charset => 'utf-8'
    return "PONG"
  end
end
