#!/usr/bin/ruby

# this script might come in handy for moving optica data
# from one ZK cluster to another

require 'zk'

dest_zk = 'new-zk:2181/optica'
source_zk = 'old-zk:2181/optica'

puts "connecting to dest"
dest = ZK.new(dest_zk)
dest.ping?

puts "connecting to source"
source = ZK.new(source_zk)
source.ping?

source.children('/').each do |child|
  child = "/#{child}"

  puts "reading #{child}"
  data, stat = source.get(child)

  begin
    dest.set(child, data)
  rescue ZK::Exceptions::NoNode => e
    dest.create(child, :data => data)
  end
  puts "wrote #{child}"
end
