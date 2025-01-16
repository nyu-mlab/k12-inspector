SELECT
	school_name,
        base_hostname,
        url_to_visit
FROM crawl_queue
WHERE result_webpage_id = 'skipped:different_base_hostname';
