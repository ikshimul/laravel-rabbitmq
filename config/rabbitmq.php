<?php

return  [
    'rabbitmq_host' => env('RABBITMQ_HOST', 'localhost'),
    'rabbitmq_port' => env('RABBITMQ_PORT', 5672),
    'rabbitmq_user' => env('RABBITMQ_USER', 'guest'),
    'rabbitmq_password' => env('RABBITMQ_PASSWORD', 'guest'),
    'rabbitmq_vhost' => env('RABBITMQ_VHOST', '/'),
];
