# :nodoc: namespace
module Tem::Mr::Search
  
class Db
  attr_reader :data
  def initialize(path)
    @data = File.open(path, 'r') { |f| YAML.load f }
  end
end
  
end  # namespace Tem::Mr::search
