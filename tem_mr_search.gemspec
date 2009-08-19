# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{tem_mr_search}
  s.version = "0.2.2"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.2") if s.respond_to? :required_rubygems_version=
  s.authors = ["Victor Costan"]
  s.date = %q{2009-08-19}
  s.default_executable = %q{tem_mr_search_server}
  s.description = %q{Tem Map-Reduce proof of concept: database search.}
  s.email = %q{victor@costan.us}
  s.executables = ["tem_mr_search_server"]
  s.extra_rdoc_files = ["bin/tem_mr_search_server", "CHANGELOG", "lib/tem_mr_search/client.rb", "lib/tem_mr_search/client_query.rb", "lib/tem_mr_search/db.rb", "lib/tem_mr_search/map_reduce_executor.rb", "lib/tem_mr_search/map_reduce_job.rb", "lib/tem_mr_search/map_reduce_planner.rb", "lib/tem_mr_search/query_builder.rb", "lib/tem_mr_search/server.rb", "lib/tem_mr_search/web_client_query_builder.rb", "lib/tem_mr_search.rb", "LICENSE", "README"]
  s.files = ["bin/tem_mr_search_server", "CHANGELOG", "lib/tem_mr_search/client.rb", "lib/tem_mr_search/client_query.rb", "lib/tem_mr_search/db.rb", "lib/tem_mr_search/map_reduce_executor.rb", "lib/tem_mr_search/map_reduce_job.rb", "lib/tem_mr_search/map_reduce_planner.rb", "lib/tem_mr_search/query_builder.rb", "lib/tem_mr_search/server.rb", "lib/tem_mr_search/web_client_query_builder.rb", "lib/tem_mr_search.rb", "LICENSE", "Manifest", "Rakefile", "README", "tem_mr_search.gemspec", "test/mr_test_case.rb", "test/test_client_server.rb", "test/test_db.rb", "test/test_map_reduce_executor.rb", "test/test_map_reduce_job.rb", "test/test_map_reduce_planner.rb", "test/test_query_builders.rb", "testdata/cluster.yml", "testdata/empty_cluster.yml", "testdata/fares.yml", "testdata/parallel_plan_431.yml", "testdata/parallel_plan_740.yml", "testdata/serial_plan_410.yml", "testdata/serial_plan_431.yml", "testdata/serial_plan_740.yml"]
  s.homepage = %q{http://tem.rubyforge.org}
  s.rdoc_options = ["--line-numbers", "--inline-source", "--title", "Tem_mr_search", "--main", "README"]
  s.require_paths = ["lib"]
  s.rubyforge_project = %q{tem}
  s.rubygems_version = %q{1.3.5}
  s.summary = %q{Tem Map-Reduce proof of concept: database search.}
  s.test_files = ["test/test_client_server.rb", "test/test_db.rb", "test/test_map_reduce_executor.rb", "test/test_map_reduce_job.rb", "test/test_map_reduce_planner.rb", "test/test_query_builders.rb"]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<tem_ruby>, [">= 0.11.2"])
      s.add_runtime_dependency(%q<tem_multi_proxy>, [">= 0.2"])
    else
      s.add_dependency(%q<tem_ruby>, [">= 0.11.2"])
      s.add_dependency(%q<tem_multi_proxy>, [">= 0.2"])
    end
  else
    s.add_dependency(%q<tem_ruby>, [">= 0.11.2"])
    s.add_dependency(%q<tem_multi_proxy>, [">= 0.2"])
  end
end
