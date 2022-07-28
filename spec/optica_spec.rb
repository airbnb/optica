require 'spec_helper'
require './optica.rb'

RSpec.describe Optica do
  def app
    Optica
  end

  before(:all) do
    Optica.set :logger, double('TestLogger')
  end

  describe '/ping' do
    it 'returns PONG' do
      get '/ping'
      expect(last_response.status).to eq(200)
      expect(last_response.body).to eq('PONG')
    end
  end

  describe '/' do
    let(:data) {
      {
        'ip' => '127.0.0.1',
        'id' => 'test',
        'environment' => 'development',
      }
    }
    let(:object_data) {
      {
        'ip' => '127.0.0.1',
        'test' => Object.new,
      }
    }

    before(:all) do
      Optica.set :ip_check, :test
    end

    before(:each) do
      Optica.set :store, double('TestStore')
      event = double('TestEvent')
      allow(event).to receive(:name).and_return('TestEvent')
      Optica.set :events, [event]
      statsd = double('TestStatsd')
      allow(statsd).to receive(:increment)
      stub_const('STATSD', statsd)
    end

    it 'can post data' do
      expect(Optica.store).to receive(:add).with(data['ip'], data).and_return(data)
      expect(Optica.events.first).to receive(:send)
      post('/', Oj.dump(data), 'CONTENT_TYPE' => 'application/json')
      expect(last_response.status).to eq(200)
      expect(last_response.body).to eq('stored')
    end

    it 'does not load objects' do
      loaded_data = nil
      allow(Optica.store).to receive(:add) do |ip, data|
        loaded_data = data
      end
      post('/', Oj.dump(object_data), 'CONTENT_TYPE' => 'application/json')
      expect(loaded_data).not_to be_nil
      expect(loaded_data['test']).to be_a(Hash)
    end
  end
end
