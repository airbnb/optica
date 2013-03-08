require 'json'
opts = JSON.parse( File.read('config.json') )

# configure the store
require './store.rb'
store = Store.new(opts)
store.start

# start the app
require './optica.rb'
Optica.set :store, store
run Optica
