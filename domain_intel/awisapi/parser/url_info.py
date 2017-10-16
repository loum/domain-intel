""":class:`UrlInfo`

"""
import json

from logga import log


class UrlInfo(object):
    """Abstacts an AWIS UrlInfo action.

    .. attribute:: domain

    .. attribute:: online_since

    .. attribute:: domain_title

    .. attribute:: domain_description

    .. attribute:: domain_rank
        Global domain rank

    .. attribute:: domain_rank_by_country
        List of domain rank by country

    .. attribute:: median_load_time

    .. attribute:: speed_percentile

    .. attribute:: related_links

    .. attribute:: contributing_subdomains

    """
    def __init__(self, record):
        """Parse and de-construct a *record* which is a JSON bytes string.

        """
        self.__raw = json.loads(record.decode('utf-8'))
        self.__data = self.__raw['UrlInfoResult']['Alexa']
        self.__content_data = self.__data.get('ContentData')
        self.__content_site = self.__content_data.get('SiteData')
        self.__traffic_data = self.__data.get('TrafficData', {})
        self.__related = self.__data.get('Related', {})

    def __call__(self):
        """Just dump the raw JSON.
        """
        return {
            'title': self.domain_title,
            'online_since': self.online_since,
            'median_load_time': self.median_load_time,
            'speed_percentile': self.speed_percentile,
            'adult_content': self.adult_content,
            'links_in_count': self.links_in_count,
            'locale': self.locale,
            'encoding': self.encoding,
            'description': self.domain_description,
            'rank': self.domain_rank,
        }

    @property
    def raw(self):
        """Just dump the raw JSON.
        """
        return self.__raw

    @property
    def domain(self):
        """Return the :attr:`domain`
        """
        return self.__content_data.get('DataUrl').get('$')

    @property
    def online_since(self):
        """Return the :attr:`domain`
        """
        online_since_value = None
        online_since = self.__content_site.get('OnlineSince')
        if online_since is not None:
            online_since_value = online_since.get('$')

        return online_since_value

    @property
    def median_load_time(self):
        """Return the :attr:`domain`
        """
        speed = self.__content_data.get('Speed', {'MedianLoadTime': {}})
        load_time = speed.get('MedianLoadTime', {'$': None})

        return load_time.get('$')

    @property
    def speed_percentile(self):
        """Return the :attr:`speed_percentile`
        """
        speed = self.__content_data.get('Speed', {'Percentile': {}})
        percentile = speed.get('Percentile', {'$': None})

        return percentile.get('$')

    @property
    def adult_content(self):
        """Whether the site has adult content.
        """
        value = self.__content_data.get('AdultContent', {'$': None})

        return value.get('$') is not None and value.get('$') == 'yes'

    @property
    def links_in_count(self):
        """Sites that link to this domain.
        """
        count = self.__content_data.get('LinksInCount', {'$': None})

        return count.get('$')

    @property
    def locale(self):
        """Site locale
        """
        language = self.__content_data.get('Language', {'Locale': {}})
        locale = language.get('Locale', {'$': None})

        return locale.get('$')

    @property
    def encoding(self):
        """Site encoding
        """
        language = self.__content_data.get('Language', {'Encoding': {}})
        encoding = language.get('Encoding', {'$': None})

        return encoding.get('$')

    @property
    def domain_title(self):
        """Return the :attr:`domain_title`
        """
        return self.__content_site.get('Title').get('$')

    @property
    def domain_description(self):
        """Return the :attr:`domain_description`
        """
        description = None
        if self.__content_site.get('Description') is not None:
            description = self.__content_site.get('Description').get('$')

        return description

    @property
    def domain_rank(self):
        """Return the :attr:`domain_rank`
        """
        return self.__traffic_data.get('Rank').get('$')

    @property
    def domain_rank_by_country(self):
        """Return the :attr:`domain_rank_by_country`
        """
        countries = self.__traffic_data.get('RankByCountry', {})
        country_ranks = countries.get('Country', [])
        if not isinstance(country_ranks, list):
            country_ranks = [country_ranks]
        for country in country_ranks:
            if country.get('@Code') == 'O':
                continue

            yield (country.get('@Code'), country.get('Rank').get('$'))

    @property
    def related_links(self):
        """Return the :attr:`related_links`
        """
        links = self.__related.get('RelatedLinks', {'RelatedLink': {}})
        related_links = links.get('RelatedLink', [])
        if not isinstance(related_links, list):
            related_links = [related_links]
        for link in related_links:
            data_url = link.get('DataUrl').get('$')
            nav_url = link.get('NavigableUrl').get('$')
            title = link.get('Title').get('$')

            yield (data_url, nav_url, title)

    @property
    def contributing_subdomains(self):
        """Return the :attr:`contributing_subdomains`
        """
        contributing = self.__traffic_data.get('ContributingSubdomains', {})
        subdomains = contributing.get('ContributingSubdomain', [])
        if not isinstance(subdomains, list):
            subdomains = [subdomains]

        for subdomain in subdomains:
            data_url = subdomain.get('DataUrl').get('$')
            if data_url == 'OTHER':
                continue

            time_range = subdomain.get('TimeRange', {'Months': {}})
            months = time_range.get('Months').get('$')

            reach = subdomain.get('Reach', {'Percentage': {}})
            reach_pc = reach.get('Percentage').get('$')
            if isinstance(reach_pc, str) or isinstance(reach_pc, unicode):
                reach_pc = float(reach_pc.rstrip('%'))

            page_views = subdomain.get('PageViews', {})
            page_views_pc = page_views.get('Percentage').get('$')
            if (isinstance(page_views_pc, str) or
                    isinstance(page_views_pc, unicode)):
                page_views_pc = float(page_views_pc.rstrip('%'))
            page_views_per_user = page_views.get('PerUser').get('$')

            yield (data_url, months, reach_pc, page_views_pc, page_views_per_user)
