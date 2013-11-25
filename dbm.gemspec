# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

require 'rexml/document'
require 'rexml/xpath'

doc = REXML::Document.new File.new('pom.xml')
DBM_VERSION = REXML::XPath.first(doc, "//project/version").text

files = `git ls-files -- lib/* spec/* sample/*`.split("\n")
files << 'lib/dbm.jar'
files << 'lib/mapdb.jar'

Gem::Specification.new do |s|
  s.name        = 'dbm'
  s.version     = DBM_VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = 'Thomas E. Enebo'
  s.email       = 'tom.enebo@gmail.com'
  s.homepage    = 'http://github.com/jruby/dbm'
  s.summary     = %q{DBM extension}
  s.description = %q{DBM extension}
  s.files         = files
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ["lib"]
  s.has_rdoc      = true
end
