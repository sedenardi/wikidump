CREATE TABLE `page` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `page_id` int(10) unsigned NOT NULL,
  `page_title` varbinary(255) NOT NULL DEFAULT '',
  `page_is_redirect` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `page_len` int(10) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  INDEX `idx_page_title` (`page_title`),
  INDEX `idx_page_id` (`page_id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE TABLE `redirect` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `rd_from` int(10) unsigned NOT NULL,
  `rd_title` varbinary(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  INDEX `idx_redirect_from` (`rd_from`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE TABLE `links` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `from` int(10) unsigned NOT NULL,
  `to` int(10) unsigned NOT NULL,
  `redirected` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=binary;