require 'sinatra/base'

class Optica < Sinatra::Base
  get '/' do
      "no nodes registered yet!"
  end

  post '/' do
    "you got it, buddy"
  end
end
