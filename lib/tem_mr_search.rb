require 'rubygems'
require 'tem_multi_proxy'
require 'tem_ruby'

# :nodoc: namespace
module Tem::Mr
end
# :nodoc: namespace
module Tem::Mr::Search
end

require 'tem_mr_search/client.rb'
require 'tem_mr_search/db.rb'
require 'tem_mr_search/map_reduce_executor.rb'
require 'tem_mr_search/map_reduce_job.rb'
require 'tem_mr_search/map_reduce_planner.rb'
require 'tem_mr_search/query_builder.rb'
require 'tem_mr_search/client_query.rb'
require 'tem_mr_search/server.rb'
require 'tem_mr_search/web_client_query_builder.rb'
