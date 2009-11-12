require 'rubygems'
gem 'echoe'
require 'echoe'

Echoe.new('tem_mr_search') do |p|
  p.project = 'tem'  # rubyforge project
  p.docs_host = "costan@rubyforge.org:/var/www/gforge-projects/tem/rdoc/"
  
  p.author = 'Victor Costan'
  p.email = 'victor@costan.us'
  p.summary = 'Tem Map-Reduce proof of concept: database search.'
  p.url = 'http://tem.rubyforge.org'
  p.dependencies = ['tem_ruby >=0.14.0', 'tem_multi_proxy >=0.2.6']
  p.dependencies = ['tem_ruby >=0.11.7', 'tem_multi_proxy >=0.2.6']
  p.development_dependencies = ["echoe >=3.2",
                                "flexmock >=0.8.6"]
  
  p.need_tar_gz = !Gem.win_platform?
  p.need_zip = !Gem.win_platform?
  p.rdoc_pattern = /^(lib|bin|tasks|ext)|^BUILD|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/  
end

if $0 == __FILE__
  Rake.application = Rake::Application.new
  Rake.application.run
end
