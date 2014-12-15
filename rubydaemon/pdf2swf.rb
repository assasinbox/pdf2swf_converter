require 'rubygems'
require 'daemons'
require 'fileutils'
require 'flash_tool'
require 'pdf/reader'
require 'net/http'
require 'uri'

STACK_DIR = "/var/pdffiller/stack"
dir = File.expand_path(File.join(File.dirname(__FILE__)))
daemon_options = {
  :multiple   => true,
  :dir_mode   => :normal,
  :dir        => File.join(dir, 'tmp'),
  :backtrace  => true,
  :log_output => true
}
class PageReceiver
  attr_accessor :pages
  def page_count(arg)
    @pages = arg
  end
end
def original_file_path(file)
  begin
    file_path = File.new(file).gets
    file_path.scan(/(.+)/)[0][0]
  rescue
    nil 
  end
end
def original_file_name(path)
  path.scan(/.+\/(.+)/)[0][0]
end
def original_dir(path)
  path.scan(/(.+)\/.+/)[0][0]
end
def prepare_dir_structure(base_dir, total_page_count)
  (0..(total_page_count-1)).each do |dir_name|    
    unless File.exist?("#{base_dir}/#{dir_name}")
      FileUtils.mkdir("#{base_dir}/#{dir_name}")
      system "chmod 775 #{base_dir}/#{dir_name}" 
      system "chown developer:apache #{base_dir}/#{dir_name}"
    end
  end
end
def total_page_count(original_file)
  receiver = PageReceiver.new
  pdf = PDF::Reader.file("#{original_file}", receiver, :pages => false)
  receiver.pages  
end
def check_status(original_file_dir)
  if File.exist?("#{original_file_dir}/thumbs.swf") and File.exist?("#{original_file_dir}/doc.swf")
    status_success(original_file_dir)
  else
    status_error(original_file_dir)
  end
end
def send_status(original_file_dir, status)
  begin
    unless (original_file_dir =~ /sts\/.+\/.+\/(.+)/).nil?
      project_id = original_file_dir.scan(/sts\/.+\/.+\/(.+)/).first.first
      url = URI.parse('http://www.sendtosign.com')
      res = Net::HTTP.start(url.host, url.port) {|http|
        http.get("/tosign.php?status=#{status}&project_id=#{project_id}")
      }
      puts "sending status to www.sendtosign.com/tosign.php?status=#{status}&project_id=#{project_id}"
      puts res.body
      puts url.host
      #Net::HTTP.get('http://www.sendtosign.com', "tosign.php?status=#{status}&project_id=#{project_id}")
    end
  rescue => e
    puts e.message
  end
end

def status_error(original_file_dir, message = "Error: Daemon can't convert this file")
  send_status(original_file_dir, "NO")
  File.open("#{original_file_dir}/convert.status", 'w') {|f| f.write(message) }
end
def status_success(original_file_dir)
  send_status(original_file_dir, "OK")
  FileUtils.touch "#{original_file_dir}/convert.ready"
end

Daemons.run_proc('pdf2swf', daemon_options) do
  loop do
    files = Dir.glob("#{STACK_DIR}/*")
    files.each do |file|
      begin
        original_file_path = original_file_path(file)               
        unless original_file_path.nil?                              
          puts "#{original_file_path} converting started"
          FileUtils.rm "#{file}"
          original_file_dir = original_dir(original_file_path)
          total_page_count = total_page_count(original_file_path)
	  puts "#{Time.now} Prepare dir structure started"
          prepare_dir_structure(original_dir(original_file_path), total_page_count)
          puts "#{Time.now} Prepare dir structure finished"
          puts "#{Time.now} Generating thumbs started"
          puts "executing pdf2swf -T 9 -s zoom=20 #{original_file_path} #{original_file_dir}/thumbs.swf"
          system "pdf2swf -T 9 -s zoom=20 #{original_file_path} #{original_file_dir}/thumbs.swf"
          puts "#{Time.now} Generating thumbs finished"
          puts "#{Time.now} Generating main swf started"
	  puts "executing pdf2swf -T 9 -f -s zoom=96 #{original_file_path} #{original_file_dir}/doc.swf"
          system "pdf2swf -T 9 -f -s zoom=96 #{original_file_path} #{original_file_dir}/doc.swf"
          puts "#{Time.now} Generating main swf finished"          
          puts "#{original_file_path} converting finished"
          check_status(original_file_dir)
        end
      rescue => e
        status_error(original_file_dir,e.message)
        puts e.message
      end
    end
    sleep 0.5
  end
end
