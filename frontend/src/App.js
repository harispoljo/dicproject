import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import axios from "axios";

const api = axios.create({
  baseURL: `http://localhost:8080`
})


class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
     data: []
   }

    this.getData = this.getData.bind(this);

  }

  getData() {
  api.get('/api/data').then(res => {
    console.log("getnode", res.data.rows)
    this.setState({
      data: res.data.rows
    });
  })
}


  componentDidMount() {
    let intervalId = setInterval(this.getData, 5000);
    this.setState({id:intervalId})
  }

  componentWillUnmount(){
    clearInterval(this.state.id);

  }

  render() {

    return (
      <div className="App">
        <div className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h2>DIC PROJECT - CORONA</h2>
        </div >
        <div className="Data" >
        {this.state.data.sort((a, b) => parseFloat(a.corona_cases) - parseFloat(b.corona_cases)).map(item => resource(item.country, item.corona_cases))}
        </div>
      </div>
    );
  }
}

function resource(countryname, casedensity) {
  if (parseFloat(casedensity) > 100){
    return <div key={parseFloat(casedensity)} style={{color:"red", display:"block"}}><h2 style={{display:"inline"}}>{countryname+" "+parseFloat(casedensity).toFixed(2)}</h2> </div>
  }
  else
    return <div key={parseFloat(casedensity)} style={{color:"blue", display:"block"}}><h2 style={{display:"inline"}}>{countryname+" "+parseFloat(casedensity).toFixed(2)}</h2> </div>

}
export default App;
