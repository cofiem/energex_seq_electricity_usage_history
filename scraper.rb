require 'scraperwiki'
require 'mechanize'
require 'nokogiri'
require 'active_support'
require 'active_support/core_ext'
require './outages'

Time.zone = 'Brisbane'

URI_DEMAND = 'https://www.energex.com.au/static/Energex/Network%20Demand/networkdemand.txt'
URI_OUTAGES = 'https://www.energex.com.au/power-outages/emergency-outages'

outages_helper = Outages.new
current_time = Time.zone.now

# Get and save demand
open(URI_DEMAND) do |i|
  demand_page = i.read
  demand_hash = outages_helper.get_demand(demand_page, current_time)
  ScraperWiki.save_sqlite([:retrieved_at], demand_hash, 'demand')
end


# Get and save outages and summary info
open(URI_OUTAGES) do |i|
  outages_page = i.read

  # get and save outages
  outages = outages_helper.get_outages(outages_page)
  outages.each do |outage|
    ScraperWiki.save_sqlite([:retrieved_at], outage, 'data')
  end

  # get and save summary
  summary = outages_helper.get_summary(outages_page, current_time)
  ScraperWiki.save_sqlite([:retrieved_at], summary, 'summary')

end
