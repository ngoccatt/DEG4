import {AiOutlineLogout} from "react-icons/ai"
import {BsEmojiSmile, BsEmojiNeutral, BsEmojiFrown} from "react-icons/bs"
import { Link } from "react-router-dom"
const Header = ({title}) => {
    return(

        <div style={{marginLeft: "15%"}} className="row position-sticky top-0 py-3 shadow-sm bg-white">
        <div className="col">
            <span className="me-2 ms-2 text-danger"><BsEmojiSmile size={20} /></span>
            <span className="me-2 text-success"><BsEmojiNeutral size={20} /></span>
            <span className="me-2 text-primary"><BsEmojiFrown size={20} /></span>
        </div>
        <div className="col d-flex justify-content-end pe-5">
        <Link to="/">  <span data-bs-toggle="tooltip" data-bs-placement="top" title="Logout" className="text-dark ">   <AiOutlineLogout size={20} />  </span> </Link> 
        </div>
        </div>
    )
}

export default Header