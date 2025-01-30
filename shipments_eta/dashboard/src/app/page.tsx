"use client";
import axios from "axios";
import { useState, useEffect } from 'react';
import { Card, BarChart, BarList } from "@tremor/react";

interface RouteStatusItem {
  route: string;
  status: string;
  status_count: number;
}

const TINYBIRD_API_BASE = "https://api.us-east.tinybird.co/v0/pipes/Shipping.json";
const TINYBIRD_TOKEN = "xyz"
const URL = `${TINYBIRD_API_BASE}?token=${TINYBIRD_TOKEN}`

const fetchTinybirdData = async () => {
  const response = await fetch(URL);
  return response.json();
};

const fetchTinybirdDataRoutePerformance = async () => {
  const response = await axios.get("https://api.us-east.tinybird.co/v0/pipes/route_perf.json", {
    params: { token: TINYBIRD_TOKEN },
  });
  return response.data.data;
};

const fetchTinybirdDataRouteStatusStats = async () => {
  const response = await axios.get("https://api.us-east.tinybird.co/v0/pipes/route_staus_stats.json", {
    params: { token: TINYBIRD_TOKEN },
  });
  return response.data.data;
};

const dataFormatter = (number: number) =>
  Intl.NumberFormat('us').format(number).toString();

export default function Example() {
  const [showComparison, setShowComparison] = useState(false);
  const [topDelayedCustomersFiltered, setTopDelayedCustomersFiltered] = useState([]);
  const [dataRoutePerformanceFiltered, setDataRoutePerformanceFiltered] = useState([]);
  const [dataRouteStatusStats, setDataRouteStatusStats] = useState<RouteStatusItem[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      // Fetch customer data
      const customerData = await fetchTinybirdData();
      const topDelayedCustomers = customerData.data.map((item: any) => ({
        name: item.customer_id,
        value: Number(item.total_delay_minutes)
      }));
      const filteredCustomers = topDelayedCustomers
        .filter((item: any) => item.value > 0)
        .sort((a: any, b: any) => b.value - a.value)
        .slice(0, 5);
      setTopDelayedCustomersFiltered(filteredCustomers);

      // Fetch route performance data
      const routePerformance = await fetchTinybirdDataRoutePerformance();
      const mappedRoutePerformance = routePerformance.map((item: any) => ({
        name: item.route,
        value: Number(item.avg_delay_minutes)
      }));
      const filteredRoutePerformance = mappedRoutePerformance
        .filter((item: any) => item.value > 0)
        .sort((a: any, b: any) => b.value - a.value)
        .slice(0, 5);
      setDataRoutePerformanceFiltered(filteredRoutePerformance);

      // Fetch route status stats
      const routeStatusStats = await fetchTinybirdDataRouteStatusStats();
      setDataRouteStatusStats(routeStatusStats);
    };

    fetchData();
  }, []);

  return (
    <div className="flex gap-2 p-5 flex-wrap">
      <h1 className="text-2xl font-bold text-center w-full">Shipments Dashboard</h1>
      <div className="flex gap-2 w-full">
        <Card className="w-1/2">
          <h3 className="ml-1 mr-1 font-semibold text-tremor-content-strong dark:text-dark-tremor-content-strong">
            Top Delayed Customers (minutes)
          </h3>
          <BarList
            data={topDelayedCustomersFiltered}
          />
        </Card>
        <Card className="w-1/2">
          <h3 className="ml-1 mr-1 font-semibold text-tremor-content-strong dark:text-dark-tremor-content-strong">
            Average Delay by Route (minutes)
          </h3>
          <BarList
            data={dataRoutePerformanceFiltered}
          />
        </Card>
      </div>
      <div className="flex gap-2 w-full">
        {([...new Set(dataRouteStatusStats.map((item: any) => item.route))] as string[]).map((routeName: string) => {
          const routeData = dataRouteStatusStats.filter((item: any) => item.route === routeName);
          // Define fixed order of statuses based on the Python code
          const statusOrder = ["Pending", "In Transit", "At Checkpoint", "Delayed", "Delivered", "Canceled"];
          
          // Create ordered data with 0 counts for missing statuses
          const orderedData = statusOrder.map(status => {
            const matchingItem = routeData.find((item: any) => item.status === status);
            return {
              status,
              value: matchingItem ? matchingItem.status_count : 0
            };
          });

          return (
            <Card key={routeName} className="w-1/3">
              <h3 className="ml-1 mr-1 font-semibold text-tremor-content-strong dark:text-dark-tremor-content-strong">
                {routeName} Status Distribution
              </h3>
              <BarChart
                className="h-72 mt-4"
                data={orderedData}
                index="status"
                categories={["value"]}
                colors={["emerald", "amber", "rose", "indigo", "cyan", "violet"]}
                showLegend={false}
                valueFormatter={dataFormatter}
                rotateLabelX={{angle: 45, verticalShift: 25, xAxisHeight: 70}}
              />
            </Card>
          );
        })}
      </div>
    </div>
  );
}