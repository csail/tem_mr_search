# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{tem_mr_search}
  s.version = "0.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 1.2") if s.respond_to? :required_rubygems_version=
  s.authors = ["Victor Costan"]
  s.date = %q{2009-05-31}
  s.description = %q{Tem Map-Reduce proof of concept: corpus search.}
  s.email = %q{victor@costan.us}
  s.extra_rdoc_files = ["CHANGELOG", "lib/tem_mr_search/client_query.rb", "lib/tem_mr_search/db.rb", "lib/tem_mr_search/query.rb", "lib/tem_mr_search/query_builder.rb", "lib/tem_mr_search/query_state.rb", "lib/tem_mr_search.rb", "LICENSE", "README"]
  s.files = ["CHANGELOG", "lib/tem_mr_search/client_query.rb", "lib/tem_mr_search/db.rb", "lib/tem_mr_search/query.rb", "lib/tem_mr_search/query_builder.rb", "lib/tem_mr_search/query_state.rb", "lib/tem_mr_search.rb", "LICENSE", "Manifest", "Rakefile", "README", "test/mr_test_case.rb", "test/test_db.rb", "test/test_query.rb", "test/test_query_builder.rb", "testdata/fares.yml", "tem_mr_search.gemspec"]
  s.homepage = %q{http://tem.rubyforge.org}
  s.rdoc_options = ["--line-numbers", "--inline-source", "--title", "Tem_mr_search", "--main", "README"]
  s.require_paths = ["lib"]
  s.rubyforge_project = %q{tem}
  s.rubygems_version = %q{1.3.4}
  s.summary = %q{Tem Map-Reduce proof of concept: corpus search.}
  s.test_files = ["test/test_db.rb", "test/test_query.rb", "test/test_query_builder.rb"]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<tem_ruby>, [">= 0.10.2"])
    else
      s.add_dependency(%q<tem_ruby>, [">= 0.10.2"])
    end
  else
    s.add_dependency(%q<tem_ruby>, [">= 0.10.2"])
  end
end
