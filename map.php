<?php
$locations = array();
$row = 1;
if(($handle = fopen("interview-exercise-data.csv", "r")) !== FALSE){
  while(($data = fgetcsv($handle, 1000, ",")) !== FALSE){
    $num = count($data);
    if($row > 1){
      $location = array(
        'Latitude' => $data[3], 
        'Longitude' => $data[4] 
        );
      array_push($locations, $location);
    }
    $row++;
  }
  fclose($handle);
}

$JScode = "";
foreach($locations as &$point){
  $JScode .= "new google.maps.LatLng(".$point['Latitude'].", ".$point['Longitude']."),";
}

$total = count($locations);
$mapCentre = array('Latitude' => (($locations[0]['Latitude']+$locations[$total-1]['Latitude'])/2), 'Longitude' => (($locations[0]['Longitude']+$locations[$total-1]['Longitude'])/2));
?>
<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="initial-scale=1.0, user-scalable=no">
  <meta charset="utf-8">
  <title>A line on map</title>
  <style>
    html, body, #map-canvas {
      height: 100%;
      margin: 0px;
      padding: 0px
    }
  </style>
  <script src="https://maps.googleapis.com/maps/api/js?v=3.exp&signed_in=true"></script>
  <script>
    function initialize() {
      var mapOptions = {
        zoom: 14,
        center: new google.maps.LatLng(<?php echo $mapCentre['Latitude']; ?>, <?php echo $mapCentre['Longitude']; ?>),
        mapTypeId: google.maps.MapTypeId.TERRAIN
      };

      var map = new google.maps.Map(document.getElementById('map-canvas'),
        mapOptions);

      var flightPlanCoordinates = [
      <?php echo $JScode; ?>
      ];
      var flightPath = new google.maps.Polyline({
        path: flightPlanCoordinates,
        geodesic: true,
        strokeColor: '#FF0000',
        strokeOpacity: 1.0,
        strokeWeight: 3
      });

      flightPath.setMap(map);
    }

    google.maps.event.addDomListener(window, 'load', initialize);
  </script>
</head>
<body>
  <div id="map-canvas"></div>
</body>
</html>
