import Dashboard from "../Pages/Dashboard"
import Order from "../Pages/Order"
import Login from "../Pages/Login"
import MainLayout from "../Layout/MainLayout"
import LoginLayout from "../Layout/LoginLayout"
import BestSeller from "../Pages/BestSeller"

const routers = [
    {path: '/order', component: Order, layout: MainLayout},
    {path: '/bestseller', component: BestSeller, layout: MainLayout},
    {path: '/dashboard', component: Dashboard, layout: MainLayout},
    {path: '/', component: Login, layout: LoginLayout}
]

export default routers