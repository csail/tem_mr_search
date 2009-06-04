# :nodoc: namespace
module Tem::Mr::Search
  
class Db
  attr_reader :data
  attr_reader :id_attribute
  
  def initialize(path)
    @data = File.open(path, 'r') { |f| YAML.load f }
    @id_attribute = 'flight'
  end
  
  def length
    @data.length
  end
  
  def item(item_index)
    @data[item_index]
  end
  
  def item_by_id(item_id)
    @data.find { |item| item[@id_attribute] == item_id }
  end
end
  
end  # namespace Tem::Mr::search
