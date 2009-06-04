# :nodoc: namespace
module Tem::Mr::Search

class WebClientQueryBuilder < MapReduceJob
  # Builds a client query covering preferences expressed in the Web UI.
  #
  # The supported (and required) options are:
  #   layovers_cost:: the cost of each layover
  #   start_time_cost:: the cost of the flight's departure time, in minutes
  #   duration_cost:: the cost of each minute of flying
  def self.query(options)
    QueryBuilder.query { |q|
      q.attributes :price => :tem_short, :start_time => :tem_short,
                   :end_time => :tem_short, :layovers => :tem_short
      q.id_attribute :flight
      
      # Score: 20000 - price - layover_cost * layovers -
      #                start_time * start_time_cost -
      #                (end_time - start_time) * duration_cost
      q.map { |s|
        s.ldwc 20000
        s.ldw :price
        s.sub
        s.ldw :end_time
        s.ldw :start_time
        s.sub
        s.ldwc options[:duration_cost]
        s.mul
        s.sub
        [:start_time, :layovers].each do |factor|
          s.ldw factor
          s.ldwc options[:"#{factor}_cost"]
          s.mul
          s.sub
        end
        s.stw :score
      }

      # The greater score wins.
      q.reduce { |s|
        s.ldw :score1
        s.ldw :score2
        s.cmp
        s.stw :comparison
      }
    }
  end
end
  
end  # namespace Tem::Mr::search
