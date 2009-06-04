require 'test/unit'
require 'tem_mr_search'

class MrTestCase < Test::Unit::TestCase
  include Tem::Mr::Search
  
  def setup
    super
    
    Thread.abort_on_exception = true
    
    @db_path = File.join File.dirname(__FILE__), "..", "testdata", "fares.yml"
    @cluster_file = File.join File.dirname(__FILE__), "..", "testdata",
                            "cluster.yml"
    @empty_cluster_file = File.join File.dirname(__FILE__), "..", "testdata",
                                    "empty_cluster.yml"
    @db = Db.new @db_path

    @client_query = WebClientQueryBuilder.query :layovers_cost => 1000,
                                                :start_time_cost => -1,
                                                :duration_cost => 1
  end
  
  def fare_score(fare)
    20000 + fare['start_time'] - fare['price'] - (fare['end_time'] -
        fare['start_time']) - fare['layovers'] * 1000
  end
  
  def fare_id(fare)
    fare['flight']
  end
  
  # Ensures that everything has loaded.
  def test_smoke
  end
end
