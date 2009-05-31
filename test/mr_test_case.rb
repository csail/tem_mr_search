require 'test/unit'
require 'tem_mr_search'

class MrTestCase < Test::Unit::TestCase
  include Tem::Mr::Search
  
  def setup
    super
    testdb_path = File.join File.dirname(__FILE__), "..", "testdata",
                            "fares.yml"
    @db = Db.new testdb_path

    @client_query = QueryBuilder.query { |q|
      q.attributes :price => :tem_short, :start => :tem_short,
                   :end => :tem_short
      
      # Score: 200 + start / 100 - duration - price
      q.map { |s|
        s.ldwc 200
        s.ldw :start
        s.ldbc 100
        s.div
        s.add
        s.ldw :end
        s.ldw :start
        s.sub
        s.sub
        s.ldw :price
        s.sub
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
  
  def fare_score(fare)
    200 + fare['start'] / 100 - fare['price'] - (fare['end'] - fare['start'])
  end
  
  def fare_id(fare)
    fare['flight']
  end
  
  # Ensures that everything has loaded.
  def test_smoke
  end
end
