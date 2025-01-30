SELECT 
    s.shipment_id,
    s.current_location_latitude,
    s.current_location_longitude,
    s.expected_delivery_date AS original_eta,
    s.expected_delivery_date + INTERVAL t.impact_on_ETA_minutes MINUTE AS updated_eta,
    arrayJoin(s.delays_reason, ', ') AS delay_reasons
FROM 
    shipments AS s
LEFT JOIN 
    traffic_weather AS t
ON 
    s.route_id = t.route_id
WHERE 
    s.status = 'In Transit'