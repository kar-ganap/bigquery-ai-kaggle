-- Optional seed rows (useful for a dry run if API is empty)
INSERT INTO `yourproj.ads_demo.ads_raw`
(ad_id, brand, page_name, creative_text, title, media_type, first_seen, last_seen, snapshot_url, landing_url, card_index, image_url, video_url, display_format, publisher_platforms)
VALUES
('A1','BrandA','BrandA US','Up to 30% off summer styles. Free returns.','Summer Sale','IMAGE','2025-08-01','2025-08-06','https://example.com','https://brandA.com/sale',0,'https://example.com/i.jpg',NULL,'DCO','FACEBOOK,INSTAGRAM'),
('A2','BrandA','BrandA US','Meet our breathable runners—lightweight comfort for daily miles.','New Runners','VIDEO','2025-08-03','2025-08-10','https://example.com','https://brandA.com/runners',0,NULL,'https://example.com/v.mp4','VIDEO','FACEBOOK');
