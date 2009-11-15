# Mock database for the secure Map-Reduce proof of concept.
#
# Author:: Victor Costan
# Copyright:: Copyright (C) 2009 Massachusetts Institute of Technology
# License:: MIT

# :nodoc: namespace
module Tem::Mr::Search

# Mock database for the Map-Reduce proof of concept.
class Db
  attr_reader :data
  attr_reader :id_attribute
  
  # Creates a new mock database.
  #
  # Args:
  #   path:: filesystem path to the YAML file containing the database 
  def initialize(path)
    @data = File.open(path, 'r') { |f| YAML.load f }
    @id_attribute = 'flight'
  end
  
  # The number of records in the database.
  def length
    @data.length
  end
  
  # Retrieves an item in table scan order.
  #
  # Args:
  #   item_index:: the item's 0-based index in table scan order
  #
  # Returns a hash with the item data. 
  def item(item_index)
    @data[item_index]
  end
  
  # Retrieves an item using its primary key.
  #
  # Args:
  #   item_id:: the item's primary key
  #
  # Returns a hash with the item data. 
  def item_by_id(item_id)
    @data.find { |item| item[@id_attribute] == item_id }
  end
end  # class Tem::Mr::Search::Db
  
end  # namespace Tem::Mr::Search
