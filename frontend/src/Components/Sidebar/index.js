import React from 'react';

import { Navigation } from 'react-minimal-side-navigation';
import 'react-minimal-side-navigation/lib/ReactMinimalSideNavigation.css';
import {BsFillPersonFill} from "react-icons/bs"
import { BsBagPlus } from "react-icons/bs";
import { AiFillDollarCircle } from "react-icons/ai";
import { AiOutlineAreaChart } from "react-icons/ai";
import { useNavigate } from "react-router-dom";

const SideBar = () => {
    let navigate = useNavigate()
    return (
        <div style={{width: "15%", height: "100vh"}} className=' position-fixed top-0 start-0 bg-white border-end ' >
        <div className='row mb-2 mt-4'>
            <div className='col-4 d-flex justify-content-end ml-0 '><div style={{width: "40px", height: "40px", marginTop: "13px", backgroundColor: "#2DD4BF"}} className='rounded-circle ' ><span className=' d-inline-block text-white' style={{marginLeft: "10px", marginTop: "7px" }}>HL</span> </div></div>
            <h4 className=' col text-start ms-0  mt-3 mb-4'>Admin </h4>
        </div>
        
            <Navigation 
                // you can use your own router's api to get pathname
                conso
                activeItemId={window.location.pathname}
                onSelect={({ itemId }) => {
                    navigate(itemId)
                }}
                items={[
                    {
                        title: 'Order',
                        itemId: '/order',
                        // you can use your own custom Icon component as well
                        // icon is optional
                        elemBefore: () => <BsBagPlus size={20} />,
                    },
                    {
                        title: 'Best seller',
                        itemId: '/bestseller',
                        // you can use your own custom Icon component as well
                        // icon is optional
                        elemBefore: () => <AiFillDollarCircle size={20} />,
                    },
                    {
                        title: 'Dashboard',
                        itemId: '/dashboard',
                        elemBefore: () => <AiOutlineAreaChart size={20} />,
                    
                    },
            
                ]}
            />
        </div>
    )
}

export default SideBar