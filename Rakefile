require 'bundler'
Bundler::GemHelper.install_tasks

task :default => [:build_artifact]

task :build_artifact do
  system "mvn package"
  cp File.join("target", "jruby-dbm-#{DBM_VERSION}.jar"), File.join("lib", "dbm.jar")
end
