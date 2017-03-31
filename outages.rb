class Outages

  # Get demand object from page
  # @param [String] page
  # @param [ActiveSupport:TimeWithZone] timestamp
  # @return [Hash]
  def get_demand(page, timestamp)
    value = page.to_i

    if value < 1500
      demand_rating = 1
    elsif value < 2000
      demand_rating = 2
    elsif value < 2500
      demand_rating = 3
    elsif value < 3000
      demand_rating = 4
    elsif value < 3500
      demand_rating = 5
    elsif value < 4000
      demand_rating = 6
    elsif value < 4500
      demand_rating = 7
    elsif value > 4499
      demand_rating = 8
    else
      demand_rating = nil
    end

    { demand: value, rating: demand_rating, retrieved_at: timestamp }
  end

  # Get outages from page
  # @param [String] page
  # @param [ActiveSupport:TimeWithZone] timestamp
  # @return [Hash]
  def get_outages(page)
    outages_html = Nokogiri::HTML(page.delete("\t\n\r"))
    outages_container = outages_html.css('div#unplanned-outages-wrapper')
    outages_table = outages_container.css('table#unplanned-outages-table tbody')
    outages_table_rows = outages_table.css('tr')

    items = []
    outages_table_rows.each do |tr|

      if tr.css('td').size == 1 && !tr.css('td')[0].get_attribute('colspan').nil?
        # no outages, nothing to do here
        # 'There are currently no power outages reported for South East Queensland.'

        nil
      else
        new_item = {
            title: tr['title'],
            region: tr.css('td[class=region]').text,
            suburb: tr.css('td[class=suburb]').text,
            cust: tr.css('td[class=cust]').text.to_i,
            cause: tr.css('td[class=cause]').text,
            retrieved_at: nil
        }

        # includes timezone offset
        time_to_parse = tr.css('td[class=time]')[0]
        timestamp = time_to_parse.get_attribute('data-timestamp')

        unless time_to_parse.blank?
          new_item[:retrieved_at] = Time.zone.parse(timestamp)
        end

        items.push(new_item) unless new_item.nil?
      end
    end

    items
  end

  # Get summary from page
  # @param [String] page
  # @param [ActiveSupport:TimeWithZone] timestamp
  # @return [Hash]
  def get_summary(page, timestamp)
    outages_html = Nokogiri::HTML(page)
    summary_container = outages_html.css('div#unplanned-outages-wrapper')
    summary_table_caption = summary_container.css('table#unplanned-outages-table caption')

    summary = {
        retrieved_at: nil,
        updated_at: nil,
        total_cust: nil
    }

    summary_info = summary_table_caption.text
    unless summary_info.empty?
      summary_info_regexp = /.*Last updated: (.+)Total affected customers: (\d+).*/m
      summary_info_match = summary_info_regexp.match(summary_info)
      raw_updated = summary_info_match.captures[0].strip + ' +1000'
      raw_total_customers = summary_info_match.captures[1].strip

      summary[:retrieved_at] = timestamp
      summary[:updated_at] = Time.zone.parse(raw_updated)
      summary[:total_cust] = raw_total_customers.to_i
    end

    summary
  end
end