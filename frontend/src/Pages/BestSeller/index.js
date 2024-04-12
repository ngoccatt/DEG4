import { useState, useEffect } from "react"
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import axios from "axios"

const Items = ({ current_products, handleOrder }) => {
    const images = ["https://media-cdn.tripadvisor.com/media/photo-s/1a/8e/55/ae/beef-cheek-massamun-curry.jpg",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS4PAAPW2H3q6MEgvdaY2PWnaGhDPf2w6G1nJY94L2XCA&s",
        "https://www.eligasht.co.uk/Blog/wp-content/uploads/2019/09/INDIAN-FOOD.jpg",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSOuON3J3RELs_SXYhTPVeZ3pobn4XsKoxUmp3ukmcDhA&s",
        "https://media.cnn.com/api/v1/images/stellar/prod/220526171611-11-classic-french-dishes-ratatouille.jpg?c=original",
        "https://res.cloudinary.com/rainforest-cruises/images/c_fill,g_auto/f_auto,q_auto/w_1120,h_732,c_fill,g_auto/v1661887113/indian-food/indian-food-1120x732.jpg",
        "https://d1sve9khgp0cw0.cloudfront.net/wp-content/uploads/2022/07/TnuIO7SFeYV03YQCpucl-I9Rdxo.jpg",
        "https://a.cdn-hotels.com/gdcs/production70/d1047/09e34d1b-8426-4eff-bd28-c521f617594b.jpg?impolicy=fcrop&w=800&h=533&q=medium",
        "https://www.southernliving.com/thmb/KkHZmTMmpoL409yq2CmUawAZ8Zk=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/One-Pot-Chicken-Fajita-Pasta228-2000-ad679d1f4d9049a0bf9113dde32244b8.jpg",
        "https://static01.nyt.com/images/2023/07/14/multimedia/14EVERYDAY-VEGETABLESrex2-fpml/14EVERYDAY-VEGETABLESrex2-fpml-articleLarge.jpg?quality=75&auto=webp&disable=upscale",
        "https://hips.hearstapps.com/hmg-prod/images/easter-side-dishes-candied-carrots-1647644323.png?crop=0.672xw:1.00xh;0.307xw,0&resize=980:*",
        "https://media.cnn.com/api/v1/images/stellar/prod/240208221031-biryani.jpg?c=16x9",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTxm8rxlbKthdCxOS93TIe9FmuW4RRLLhhUur9ItSE6jg&s",
        "https://i0.wp.com/nationalfoods.org/wp-content/uploads/2017/07/National-Dish-of-Germany-Sauerbraten.jpg?resize=640%2C480&ssl=1",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRDECjFYIxa5mBsF2tg-r61T5uRuyxOneNPmmqmb30ZeA&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ1k0ECD4BtjLjCH47-AqyQ5rqcBVwR23LnpAvUWaDlkg&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSKmyfBbKhJ72iVyU2tqFhLSYSlzpD8612sPP7QMwfr6A&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQezaBMgoiVDjXj1aL8kVI_NweEN_33wvY4jkjw6Wm88w&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRC8MjXmuUelziz3oWH9aVkOGk6iC9uylxNpA&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSIH171TB2TKoD4WSdE2rnwpw8cp7gxtKQPKccH3TxwNg&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSubVg8WuZVB-hi_mLo0Rcbk6JF25z7Xb-odHNfD1d_FQ&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTQF1l6UYFZwt4LUHIMze_ZUTsC6zylpFmk406YI9X4Vw&s",
        "https://gobargingwp-s3.s3.eu-west-1.amazonaws.com/wp-content/uploads/2023/02/classic-fench-dish-Confit-de-Canard-490.jpg",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcR4_h38C-qMpG14j6COLhYQ9RXtdVZlRsu0MM_f-FoxyA&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRqPhq5jmJ7YW2gMi-IXzUz0TlR0X2TaFss_tqr_mUXcg&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSr6L-cePkoJ_Vlg3APmrGHbIUPe25rqV_ReB_pk7vocg&s",
        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQVnU-27orNfPbqVP_tDABm2hLKkq5Ue4guAms2C_gp7w&s"
    ]

    // const current_products = data
    if (current_products == null) {
        return (
            <>
                <p>Loading</p>
            </>
        )
    }
    else {

        return (
            <>
                <div className="row row-cols-4" >
                    {
                        current_products && current_products.map(product => (
                            <div className="col ml-2 p-3" >
                                <div className="bg-warning m-auto p-2 rounded" style={{ height: "340px" }}>
                                    <div className="text-center">
                                        <img src={images[Math.floor(Math.random() * 26)]} style={{ width: "95%", height: "150px" }} />
                                    </div>
                                    <div className="p-2">
                                        <div className="d-flex justify-content-between">
                                            <p className="text-white">ID: {product[0]}</p>
                                            <p className="text-white">Price: {product[2].toFixed(2)} $</p>
                                        </div>
                                        <p className="text-center text-success">The amount of sales: {product[3]}</p>
                                        <p className="text-center text-white" style={{ marginTop: "-15px", height: "40px" }}>{product[1]}</p>
                                        <div className="text-center">
                                            <button className="btn btn-primary" style={{ width: "100px" }} onClick={() => handleOrder(product)}>Buy now</button>
                                        </div>
                                    </div>
                                </div>

                            </div>
                        ))
                    }
                </div>
            </>
        )
    }
}

const BestSeller = () => {
    const [bestProducts, setBestProducts] = useState([])
    const [loading, setLoading] = useState(true)
    const [update, setUpdate] = useState(true)

    const handleOrder = async (item) => {
        const customer_id = 14031
        const product_id = item[0]
        const order_id_part_first = Math.floor(Math.random() * 100)
        const order_id_part_tail = Math.floor(Math.random() * 100000)
        const order_id = order_id_part_first.toString() + order_id_part_tail.toString()
        const res = await axios(`http://localhost:8000/api/v1/order`, {
            method: "post",
            headers: {
                Accept: "application/json",
                "Content-Type": "application/json",
            },
            data: {
                orderId: order_id,
                productId: product_id,
                customerId: customer_id
            }
        })
        
        if (res.data.status == false) {
            alert("Opp! Something went wrong")
        }
        else {
            if (res.data.bestseller != null) {
                toast.success("Order successfully");
                console.log(res.data.bestseller)
                // bestProducts = res.data.bestseller
                setBestProducts(res.data.bestseller)
            }
        }
    }   

    const getBestSeller = async () => {
        setLoading(true)
        const res = await axios(`http://localhost:8000/api/v1/bestseller`, {
            method: "get",
            headers: {
                Accept: "application/json",
                "Content-Type": "application/json",
            },
        })

        if (res.data.status == false) {
            alert("Opp! Something went wrong")
        }
        else {
            if (res.data.bestseller != null) {
                setBestProducts(res.data.bestseller)
                setLoading(false)
            }
        }
    }

    useEffect(() => {
        getBestSeller()
    }, [])

    useEffect(() => {
        console.log(bestProducts)
    }, [bestProducts])

    return (
        <>
            {
                loading ? <>
                    <p>Loading ...</p>
                </> :
                    <>
                        <div className="pb-5 px-3 ">
                            <ToastContainer limit={1} />
                            <Items current_products={bestProducts} handleOrder={handleOrder} />
                        </div>
                    </>
            }
        </>
    )
}

export default BestSeller