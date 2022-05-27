FROM php:8.1.5-fpm-alpine

RUN mkdir -p /var/www/html

RUN apk --no-cache add shadow && usermod -u 1001 www-data

RUN curl -sS https://getcomposer.org/installer | php -- \
        --install-dir=/usr/local/bin --filename=composer \

RUN composer install
