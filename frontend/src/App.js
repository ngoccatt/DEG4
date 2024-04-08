import routers from './Router';
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Fragment } from 'react';
function App() {
    console.log(routers)
    return (
        <BrowserRouter>
            <Routes>
                {
                    routers.map((item) => {
                        let Layout = item.layout
                        let Page = item.component
                        if (!Layout) {
                            Layout =  Fragment
                        }
                        return (
                            <Route
                                path={item.path}
                                element={
                                    <Layout>
                                        <Page />
                                    </Layout>} />
                        )
                    })
                }
            </Routes>
        </BrowserRouter>
    );
}

export default App;
