import { useEffect, useState } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer,
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  BarChart, Bar, Rectangle, Brush,
  ReferenceLine,
} from 'recharts';
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import InfoCard from '../../Components/InfoCard';

const data = [
  { name: 'Group A', value: 400 },
  { name: 'Group B', value: 300 },
  { name: 'Group C', value: 300 },
  { name: 'Group D', value: 200 },
];

const data2 = [
  {
    name: 'Page A',
    uv: 4000,
    pv: 2400,
    amt: 2400,
  },
  {
    name: 'Page B',
    uv: 3000,
    pv: 1398,
    amt: 2210,
  },
  {
    name: 'Page C',
    uv: 2000,
    pv: 9800,
    amt: 2290,
  },
  {
    name: 'Page D',
    uv: 2780,
    pv: 3908,
    amt: 2000,
  },
  {
    name: 'Page E',
    uv: 1890,
    pv: 4800,
    amt: 2181,
  },
  {
    name: 'Page F',
    uv: 2390,
    pv: 3800,
    amt: 2500,
  },
  {
    name: 'Page G',
    uv: 3490,
    pv: 4300,
    amt: 2100,
  },
];

const data3 = [
  {
    name: 'Page A',
    uv: 4000,
    pv: 2400,
    amt: 2400,
  },
  {
    name: 'Page B',
    uv: 3000,
    pv: 1398,
    amt: 2210,
  },
  {
    name: 'Page C',
    uv: 2000,
    pv: 9800,
    amt: 2290,
  },
  {
    name: 'Page D',
    uv: 2780,
    pv: 3908,
    amt: 2000,
  },
  {
    name: 'Page E',
    uv: 1890,
    pv: 4800,
    amt: 2181,
  },
  {
    name: 'Page F',
    uv: 2390,
    pv: 3800,
    amt: 2500,
  },
  {
    name: 'Page G',
    uv: 3490,
    pv: 4300,
    amt: 2100,
  },
];

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];

const RADIAN = Math.PI / 180;
const renderCustomizedLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent, index }) => {
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);

  return (
    <text x={x} y={y} fill="white" textAnchor={x > cx ? 'start' : 'end'} dominantBaseline="central">
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

const Dashboard = () => {
  
  const [currentCard, setCurrentCard] = useState(null);

  const [RevenueCountries, setRevenueCountries] = useState(null);
  const [RevenueMonthly, setRevenueMonthly] = useState(null);
  const [RevenueYearly, setRevenueYearly] = useState(null);
  const [OrderCountries, setOrderCountries] = useState(null);
  const [OrderAvg, setOrderAvg] = useState(null);
  const [OrderTimeslot, setOrderTimeslot] = useState(null);
  const [OrderTimeslotSum, setOrderTimeslotSum] = useState(null);
  const [OrderCancelled, setOrderCancelled] = useState(null);
  const [OrderCancelledSum, setOrderCancelledSum] = useState(null);


async function fetchData(func, url) {
    // Request to get all cards from user
    try {
      let response = await fetch(url, {
      method: "GET",
      });
      let data = await response.json();

      if (response.status == 200) {
        func(data);
        console.log(data);
      }
    }
    catch (error){
      console.log(error);
    }
    
  };

function revenueSum(arr) {
  var total = 0
  for (const item of arr) {
    total += item.revenue
  }
  return total
}

function orderSum(arr) {
  var total = 0
  for (const item of arr) {
    total += item.num_orders
  }
  return total
}

  useEffect(() => {
    fetchData(setRevenueCountries, "http://localhost:8000/revenue/countries")
    fetchData(setRevenueMonthly, "http://localhost:8000/revenue/monthly")
    fetchData(setRevenueYearly, "http://localhost:8000/revenue/yearly")
    fetchData(setOrderCountries, "http://localhost:8000/orders/countries")
    fetchData(setOrderAvg, "http://localhost:8000/orders/avg")
    // fetchData(setOrderTimeslot, "http://localhost:8000/revenue/monthly")
    fetchData(setOrderTimeslotSum, "http://localhost:8000/orders/timeslot_sum")
    fetchData(setOrderCancelled, "http://localhost:8000/orders/cancelled")
    fetchData(setOrderCancelledSum, "http://localhost:8000/orders/cancelled_sum")
  }, []);

  // console.log(currentCard);

  if (
    !RevenueCountries || 
    !RevenueMonthly ||
    !RevenueYearly ||
    !OrderCountries ||
    !OrderAvg ||
    !OrderTimeslotSum ||
    !OrderCancelled ||
    !OrderCancelledSum
  ) {
    // Render a loading state
    return <p>Loading...</p>;
  }

    return (
      <ResponsiveContainer width="100%" height="50%">
        <Row style={{
          // display:'flex',
          // alignItems:'center',
          // marginBottom:"30px",
          // flexWrap: true
        }} >
          <Col style={{
            marginRight:"20px",
            marginBottom: "10px"
          }}>
            <InfoCard name={"Total Revenue"} value={revenueSum(RevenueCountries.data)} description={"Our revenue"}/>
          </Col>
          <Col style={{
            marginRight:"20px",
            marginBottom: "10px"
          }}>
            <InfoCard name={"Total Orders"} value={orderSum(OrderCountries.data)} description={"Number of order accumulated"}/>
          </Col>
          <Col style={{
            marginRight:"20px",
            marginBottom: "10px"
          }}>
            <InfoCard name={"PRICE"} value={1992} description={"went up"}/>
          </Col>
        </Row>

        <h1>Pie chart</h1>
        <PieChart width={400} height={400}>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={renderCustomizedLabel}
            outerRadius={100}
            fill="#8884d8"
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
        </PieChart>
        <LineChart
          width={500}
          height={300}
          data={data2}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="name" />
          <YAxis />
          <Tooltip />
          <Legend />
          
          <Line type="monotone" dataKey="pv" stroke="#8884d8" activeDot={{ r: 8 }} />
          <Line type="monotone" dataKey="uv" stroke="#82ca9d" />
        </LineChart>



{/* country */}
        <h1>Revenue of countries</h1>
        <BarChart
          width={500}
          height={300}
          data={RevenueCountries.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="country" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="name" height={30} stroke="#8884d8"  endIndex={5}/>
          <Bar dataKey="revenue" fill="#8884d8" activeBar={<Rectangle fill="pink" stroke="blue" />} />
        </BarChart>


{/* Yearly */}
<h1>Revenue yearly</h1>
        <BarChart
          width={500}
          height={300}
          data={RevenueYearly.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="year" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="name" height={30} stroke="#8884d8" />
          <Bar dataKey="revenue" fill="#8884d8" activeBar={<Rectangle fill="pink" stroke="blue" />} />
        </BarChart>

{/* montly */}
<h1>Revenue monthly</h1>
        <LineChart
          width={500}
          height={300}
          data={RevenueMonthly.data}
          layout={"horizontal"}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="time" height={30} stroke="#8884d8" />
          <Line dataKey="revenue" fill="#8884d8" />
        </LineChart>

        {/* Yearly */}
<h1>Top number of orders in each country</h1>
        <BarChart
          width={500}
          height={300}
          data={OrderCountries.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="country" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="country" height={30} stroke="#8884d8" endIndex={5}/>
          <Bar dataKey="num_orders" fill="#8884d8" activeBar={<Rectangle fill="pink" stroke="blue" />} />
        </BarChart>

        {/* Yearly */}
        <h1>Top average value of order and number of order in each country</h1>
        <BarChart
          width={500}
          height={300}
          data={OrderAvg.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="country" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="country" height={30} stroke="#8884d8" endIndex={5}/>
          <Bar dataKey="avg" fill="#8884d8" activeBar={<Rectangle fill="pink" stroke="blue" />} />
          <Bar dataKey="num_orders" fill="green" activeBar={<Rectangle fill="white" stroke="blue" />} />
        </BarChart>


        <h1>The number of orders in each time slot per day</h1>
        
        <BarChart
          width={500}
          height={300}
          data={OrderTimeslotSum.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="timeslot" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="timeslot" height={30} stroke="#8884d8"/>
          <Bar dataKey="num_orders" fill="#8884d8" activeBar={<Rectangle fill="pink" stroke="blue" />} />
        </BarChart>

        <h1>Cancelled orders vs successed orders in each country</h1>
        <BarChart
          width={500}
          height={300}
          data={OrderCancelled.data}
          margin={{
            top: 5,
            right: 30,
            left: 20,
            bottom: 5,
          }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="country" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Brush dataKey="country" height={30} stroke="#8884d8" endIndex={5}/>
          <Bar dataKey="success_orders" fill="#8884d8" stackId={1} activeBar={<Rectangle fill="pink" stroke="blue" />} />
          <Bar dataKey="cancel_orders" fill="#82ca9d" stackId={1} activeBar={<Rectangle fill="white" stroke="white" />} />
        </BarChart>


        <h1>Total cancelled orders vs succeed orders</h1>
        <PieChart width={400} height={400}>
          <Pie
            data={OrderCancelledSum.data}
            cx="50%"
            cy="50%"
            labelLine={false}
            isAnimationActive={true}
            label={renderCustomizedLabel}
            outerRadius={100}
            fill="#8884d8"
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip />
        </PieChart>

      </ResponsiveContainer>


    );
}

export default Dashboard; 
